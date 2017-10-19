from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import numpy as np
from math import sqrt

def mode(ls):
    r = {}
    for e in ls:
        r.setdefault(e, 0)
        r[e] += 1
    return max(r.items(), key=lambda kv: kv[1])[1]

def nback(ls):
    nums = 0
    flag = True
    for i in range(len(ls) - 1):
        if flag and ls[i] > ls[i + 1]:
            nums += 1
            flag = False
        elif not flag and ls[i] < ls[i + 1]:
            nums += 1
            flag = True
    return nums

def prc_train(line):
    id, tra, target, label = line
    points = list(map(lambda s: s.split(','), tra.split(';')[:-1]))
    points = sorted(points, key=lambda p: float(p[2]))
    xs = [float(p[0]) for p in points]
    ys = [float(p[1]) for p in points]
    ts = [float(p[2]) for p in points]
    tgt_x = float(target.split(',')[0])
    tgt_y = float(target.split(',')[1])
    xs_diff = np.diff(xs)
    ys_diff = np.diff(ys)
    ts_diff = np.diff(ts)
    speeds = xs_diff / (ts_diff + 1e-3) + 1e-3
    sp_diff = np.diff(speeds)
    xs_diff_diff = np.diff(xs_diff)
    ys_diff_diff = np.diff(ys_diff)

    nxd, nyd, nsp = mode(xs_diff), mode(ys_diff), mode(speeds)
    length = float(len(xs_diff))
    nxd_lt0, nyd_lt0, ntd_lt0 = np.sum(xs_diff < 0), np.sum(ys_diff < 0), np.sum(ys_diff < 0)
    nxbacks, nybacks, ntbacks = nback(xs), nback(ys), nback(ts)
    sp_first, sp_last, sp_mean = speeds[0], speeds[-1], np.mean(speeds)
    sp_mean_fl, sp_first_log1p, fp_last_log1p = (sp_first + sp_last) / 2, np.log1p(sp_first), np.log1p(sp_last)
    tgt_xlast_diff, tgt_ylast_diff = (tgt_x - xs[-1]), (tgt_y - ys[-1])

    fts = [float(label)]
    fts.extend([tgt_xlast_diff, tgt_ylast_diff, sqrt(tgt_xlast_diff ** 2 + tgt_ylast_diff ** 2)])
    fts.extend([np.min(xs_diff), np.max(xs_diff), np.mean(xs_diff), np.std(xs_diff),
                np.min(ys_diff), np.max(ys_diff), np.mean(ys_diff), np.std(ys_diff),
                np.min(ts_diff), np.max(ts_diff), np.mean(ts_diff), np.std(ts_diff)])
    fts.extend([nxd, nxd / length, nyd, nyd / length, nsp, nsp / length])
    fts.extend([nxd_lt0, nyd_lt0, ntd_lt0, nxd_lt0 / length, nyd_lt0 / length, ntd_lt0 / length, length + 1])
    fts.extend([nxbacks, nybacks, ntbacks])
    fts.extend([np.max(speeds), np.min(speeds), np.mean(speeds), np.std(speeds)])
    fts.extend([sp_first, sp_last, fp_last_log1p - sp_first_log1p, fp_last_log1p / sp_first_log1p])
    fts.extend([sp_mean, sp_mean_fl, sp_mean_fl - sp_mean, sp_mean_fl / sp_mean])
    fts.extend([np.max(sp_diff), np.min(sp_diff), np.std(sp_diff), np.mean(sp_diff)])
    fts.extend(
        [np.min(xs_diff_diff), np.max(xs_diff_diff), np.mean(xs_diff_diff), np.std(xs_diff_diff),
         np.min(ys_diff_diff), np.max(ys_diff_diff), np.mean(ys_diff_diff), np.std(ys_diff_diff)])
    fts.extend([np.max(xs), np.min(xs), np.std(xs), np.mean(xs)])
    fts.extend([np.max(ys), np.min(ys), np.std(ys), np.mean(ys)])

    fts = fts + [xs_diff[i] if i < len(xs_diff) else -1 for i in range(300)]
    fts = fts + [speeds[i] if i < len(speeds) else -1 for i in range(300)]

    fts_str = ' '.join([str(ft) for ft in fts])
    return fts_str.replace('nan','-1')

conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs","false")
sc = SparkContext(appName="train",conf=conf)
sqlContext = SQLContext(sc)
train_data = sc.textFile('xxxx.txt').map(lambda line: line.split(' '))
results = train_data.filter(lambda fields: len(fields[1].split(';')) - 1 >= 3).map(prc_train)
print(len(results.take(1)[0]))
results.saveAsTextFile("test")