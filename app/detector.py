"""AI 检测：本地 YOLO11n(ONNX) 识别「人」与「移动物体」。

判定逻辑
--------
1. 人形目标：COCO 类别 0(person)，置信度 >= personConf
2. 移动物体：非人目标（车、动物、包裹等常见物）置信度 >= objConf，
   且其检测框与背景差分得到的运动掩码重叠率达标，避免把静态家具误判为「移动物」
3. 命中后进入冷却期，避免同一动作反复触发
"""
import os
import time

import cv2
import numpy as np
import onnxruntime as ort

MODEL_PATH = os.environ.get("UM_MODEL", os.path.join(os.environ.get("UNICOMAI_HOME", "/opt/unicomai"), "models/yolo11n.onnx"))
INPUT_SIZE = 640

# COCO 80 类名称（用于日志与界面展示）
COCO_NAMES = [
    "person", "bicycle", "car", "motorcycle", "airplane", "bus", "train", "truck", "boat",
    "traffic light", "fire hydrant", "stop sign", "parking meter", "bench", "bird", "cat",
    "dog", "horse", "sheep", "cow", "elephant", "bear", "zebra", "giraffe", "backpack",
    "umbrella", "handbag", "tie", "suitcase", "frisbee", "skis", "snowboard", "sports ball",
    "kite", "baseball bat", "baseball glove", "skateboard", "surfboard", "tennis racket",
    "bottle", "wine glass", "cup", "fork", "knife", "spoon", "bowl", "banana", "apple",
    "sandwich", "orange", "broccoli", "carrot", "hot dog", "pizza", "donut", "cake", "chair",
    "couch", "potted plant", "bed", "dining table", "toilet", "tv", "laptop", "mouse",
    "remote", "keyboard", "cell phone", "microwave", "oven", "toaster", "sink",
    "refrigerator", "book", "clock", "vase", "scissors", "teddy bear", "hair drier",
    "toothbrush",
]

# 触发关注的类别：人 + 常见移动物（车、动物、随身物品）；家具等静物不触发
WATCH_CLASSES = {0, 1, 2, 3, 5, 7, 14, 15, 16, 17, 18, 19, 24, 26, 28}

# 室内静物（即便被检出也不触发，避免误报）
STATIC_IGNORE = {56, 57, 58, 59, 60, 62, 63, 67}


class Detector:
    def __init__(self, model_path: str = MODEL_PATH):
        self.available = os.path.exists(model_path)
        self.session = None
        if self.available:
            so = ort.SessionOptions()
            so.intra_op_num_threads = max(1, (os.cpu_count() or 2) // 2)
            so.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
            self.session = ort.InferenceSession(model_path, sess_options=so, providers=["CPUExecutionProvider"])
            self.input_name = self.session.get_inputs()[0].name
        self.prev_gray = None

    # ---------- 预处理 ----------
    @staticmethod
    def _letterbox(img, size=INPUT_SIZE):
        h, w = img.shape[:2]
        r = min(size / w, size / h)
        nw, nh = int(round(w * r)), int(round(h * r))
        resized = cv2.resize(img, (nw, nh), interpolation=cv2.INTER_LINEAR)
        canvas = np.full((size, size, 3), 114, dtype=np.uint8)
        dw, dh = (size - nw) // 2, (size - nh) // 2
        canvas[dh:dh + nh, dw:dw + nw] = resized
        return canvas, r, dw, dh

    # ---------- 推理 ----------
    def detect(self, bgr: np.ndarray, conf_thres=0.35):
        """返回 [(x1,y1,x2,y2,score,cls_id,name)]，坐标为原图坐标。"""
        if not self.available or bgr is None or bgr.size == 0:
            return []
        orig_h, orig_w = bgr.shape[:2]
        img, r, dw, dh = self._letterbox(bgr)
        blob = cv2.cvtColor(img, cv2.COLOR_BGR2RGB).astype(np.float32) / 255.0
        blob = np.transpose(blob, (2, 0, 1))[None]

        out = self.session.run(None, {self.input_name: blob})[0]
        out = np.squeeze(out, axis=0)          # (84, 8400)
        if out.shape[0] < out.shape[1]:
            out = out.T                         # (8400, 84)

        scores = out[:, 4:]
        cls_ids = scores.argmax(axis=1)
        confs = scores[np.arange(len(scores)), cls_ids]
        keep = confs >= conf_thres
        if not keep.any():
            return []
        out, confs, cls_ids = out[keep], confs[keep], cls_ids[keep]

        boxes = out[:, :4]                      # cx,cy,w,h（letterbox 坐标）
        x1 = (boxes[:, 0] - boxes[:, 2] / 2 - dw) / r
        y1 = (boxes[:, 1] - boxes[:, 3] / 2 - dh) / r
        x2 = (boxes[:, 0] + boxes[:, 2] / 2 - dw) / r
        y2 = (boxes[:, 1] + boxes[:, 3] / 2 - dh) / r
        np.clip(x1, 0, orig_w, out=x1); np.clip(x2, 0, orig_w, out=x2)
        np.clip(y1, 0, orig_h, out=y1); np.clip(y2, 0, orig_h, out=y2)

        results = []
        idxs = cv2.dnn.NMSBoxes(
            bboxes=np.stack([x1, y1, x2 - x1, y2 - y1], axis=1).tolist(),
            scores=confs.tolist(), score_threshold=conf_thres, nms_threshold=0.45,
        )
        order = np.array(idxs).flatten().tolist()
        for i in order:
            cid = int(cls_ids[i])
            results.append((float(x1[i]), float(y1[i]), float(x2[i]), float(y2[i]),
                            float(confs[i]), cid, COCO_NAMES[cid] if cid < len(COCO_NAMES) else "obj%d" % cid))
        return results

    # ---------- 运动判定 ----------
    def motion_mask(self, bgr: np.ndarray, min_area_ratio=0.002):
        """背景差分，返回运动掩码与归一化运动面积。"""
        gray = cv2.cvtColor(cv2.resize(bgr, (320, 180)), cv2.COLOR_BGR2GRAY)
        gray = cv2.GaussianBlur(gray, (5, 5), 0)
        if self.prev_gray is None:
            self.prev_gray = gray
            return None, 0.0
        d = cv2.absdiff(self.prev_gray, gray)
        self.prev_gray = gray
        _, th = cv2.threshold(d, 18, 255, cv2.THRESH_BINARY)
        th = cv2.dilate(th, np.ones((5, 5), np.uint8), iterations=2)
        return th, (th > 0).mean()

    def judge(self, bgr: np.ndarray, person_conf=0.35, obj_conf=0.45, motion_th=0.004):
        """综合判定是否触发。返回 (triggered, reason, dets, motion_ratio)。"""
        dets = self.detect(bgr, conf_thres=min(person_conf, obj_conf))
        mask, motion_ratio = self.motion_mask(bgr)
        if not dets:
            return False, "", [], motion_ratio

        h, w = bgr.shape[:2]
        persons = [d for d in dets if d[5] == 0 and d[4] >= person_conf]
        if persons:
            best = max(persons, key=lambda x: x[4])
            return True, "person:%.2f" % best[4], dets, motion_ratio

        moving = []
        for d in dets:
            if d[5] == 0 or d[5] not in WATCH_CLASSES or d[5] in STATIC_IGNORE or d[4] < obj_conf:
                continue
            if mask is None:
                moving.append(d)
                continue
            # 检测框内运动像素占比
            sx = mask.shape[1] / w
            sy = mask.shape[0] / h
            x1, y1 = int(d[0] * sx), int(d[1] * sy)
            x2, y2 = int(d[2] * sx), int(d[3] * sy)
            roi = mask[max(0, y1):max(1, y2), max(0, x1):max(1, x2)]
            if roi.size and (roi > 0).mean() > 0.06:
                moving.append(d)
        if moving:
            best = max(moving, key=lambda x: x[4])
            return True, "moving:%s:%.2f" % (best[6], best[4]), dets, motion_ratio

        # 无目标但画面整体剧烈变化（如人快速掠过、镜头被遮挡）
        if motion_ratio >= max(motion_th * 8, 0.05):
            return True, "motion_area:%.3f" % motion_ratio, dets, motion_ratio

        return False, "", dets, motion_ratio
