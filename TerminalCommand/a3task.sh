#!/bin/bash

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'


/usr/bin/hadoop fs -rm -r GROUP2/yrf7-4wry.tsv.avfout
echo 'yrf7-4wry.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/yrf7-4wry.tsv 5 
echo 'yrf7-4wry.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/yrf7-4wry.tsv.avfout AVF/yrf7-4wry.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/xjcm-e5uy.tsv.avfout
echo 'xjcm-e5uy.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/xjcm-e5uy.tsv 5 
echo 'xjcm-e5uy.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/xjcm-e5uy.tsv.avfout AVF/xjcm-e5uy.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/wvxf-dwi5.tsv.avfout
echo 'wvxf-dwi5.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/wvxf-dwi5.tsv 1 
echo 'wvxf-dwi5.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/wvxf-dwi5.tsv.avfout AVF/wvxf-dwi5.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/wng2-85mv.tsv.avfout
echo 'wng2-85mv.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/wng2-85mv.tsv 5 
echo 'wng2-85mv.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/wng2-85mv.tsv.avfout AVF/wng2-85mv.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/wibz-uqui.tsv.avfout
echo 'wibz-uqui.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/wibz-uqui.tsv 5 
echo 'wibz-uqui.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/wibz-uqui.tsv.avfout AVF/wibz-uqui.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/vza7-n6vi.tsv.avfout
echo 'vza7-n6vi.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/vza7-n6vi.tsv 5 
echo 'vza7-n6vi.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/vza7-n6vi.tsv.avfout AVF/vza7-n6vi.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ven4-h25u.tsv.avfout
echo 'ven4-h25u.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ven4-h25u.tsv 5 
echo 'ven4-h25u.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ven4-h25u.tsv.avfout AVF/ven4-h25u.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/uzcy-9puk.tsv.avfout
echo 'uzcy-9puk.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/uzcy-9puk.tsv 0.5 
echo 'uzcy-9puk.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/uzcy-9puk.tsv.avfout AVF/uzcy-9puk.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ugzk-a6x4.tsv.avfout
echo 'ugzk-a6x4.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ugzk-a6x4.tsv 5 
echo 'ugzk-a6x4.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ugzk-a6x4.tsv.avfout AVF/ugzk-a6x4.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/tbvj-mbps.tsv.avfout
echo 'tbvj-mbps.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/tbvj-mbps.tsv 5 
echo 'tbvj-mbps.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/tbvj-mbps.tsv.avfout AVF/tbvj-mbps.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/rbvx-jqnh.tsv.avfout
echo 'rbvx-jqnh.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/rbvx-jqnh.tsv 5 
echo 'rbvx-jqnh.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/rbvx-jqnh.tsv.avfout AVF/rbvx-jqnh.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/qvir-knu3.tsv.avfout
echo 'qvir-knu3.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/qvir-knu3.tsv 5 
echo 'qvir-knu3.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/qvir-knu3.tsv.avfout AVF/qvir-knu3.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/qpm9-j523.tsv.avfout
echo 'qpm9-j523.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/qpm9-j523.tsv 5 
echo 'qpm9-j523.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/qpm9-j523.tsv.avfout AVF/qpm9-j523.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/qbce-2kcu.tsv.avfout
echo 'qbce-2kcu.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/qbce-2kcu.tsv 5 
echo 'qbce-2kcu.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/qbce-2kcu.tsv.avfout AVF/qbce-2kcu.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/phth-xf25.tsv.avfout
echo 'phth-xf25.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/phth-xf25.tsv 5 
echo 'phth-xf25.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/phth-xf25.tsv.avfout AVF/phth-xf25.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ph7v-u5f3.tsv.avfout
echo 'ph7v-u5f3.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ph7v-u5f3.tsv 5 
echo 'ph7v-u5f3.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ph7v-u5f3.tsv.avfout AVF/ph7v-u5f3.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/nre2-6m2s.tsv.avfout
echo 'nre2-6m2s.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/nre2-6m2s.tsv 5 
echo 'nre2-6m2s.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/nre2-6m2s.tsv.avfout AVF/nre2-6m2s.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/kyad-zm4j.tsv.avfout
echo 'kyad-zm4j.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/kyad-zm4j.tsv 3 
echo 'kyad-zm4j.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/kyad-zm4j.tsv.avfout AVF/kyad-zm4j.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/kpav-sd4t.tsv.avfout
echo 'kpav-sd4t.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/kpav-sd4t.tsv 3 
echo 'kpav-sd4t.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/kpav-sd4t.tsv.avfout AVF/kpav-sd4t.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/khqi-x3p3.tsv.avfout
echo 'khqi-x3p3.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/khqi-x3p3.tsv 5 
echo 'khqi-x3p3.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/khqi-x3p3.tsv.avfout AVF/khqi-x3p3.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/jz4z-kudi.tsv.avfout
echo 'jz4z-kudi.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/jz4z-kudi.tsv 0.1
echo 'jz4z-kudi.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/jz4z-kudi.tsv.avfout AVF/jz4z-kudi.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/jufi-gzgp.tsv.avfout
echo 'jufi-gzgp.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/jufi-gzgp.tsv 5
echo 'jufi-gzgp.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/jufi-gzgp.tsv.avfout AVF/jufi-gzgp.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/jk35-yh5p.tsv.avfout
echo 'jk35-yh5p.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/jk35-yh5p.tsv 5
echo 'jk35-yh5p.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/jk35-yh5p.tsv.avfout AVF/jk35-yh5p.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ihfw-zy9j.tsv.avfout
echo 'ihfw-zy9j.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ihfw-zy9j.tsv 5
echo 'ihfw-zy9j.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ihfw-zy9j.tsv.avfout AVF/ihfw-zy9j.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/hg3c-2jsy.tsv.avfout
echo 'hg3c-2jsy.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/hg3c-2jsy.tsv 5
echo 'hg3c-2jsy.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/hg3c-2jsy.tsv.avfout AVF/hg3c-2jsy.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/gi8d-wdg5.tsv.avfout
echo 'gi8d-wdg5.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/gi8d-wdg5.tsv 1
echo 'gi8d-wdg5.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/gi8d-wdg5.tsv.avfout AVF/gi8d-wdg5.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/dtfq-bfpc.tsv.avfout
echo 'dtfq-bfpc.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/dtfq-bfpc.tsv 5
echo 'dtfq-bfpc.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/dtfq-bfpc.tsv.avfout AVF/dtfq-bfpc.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/dg92-zbpx.tsv.avfout
echo 'dg92-zbpx.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/dg92-zbpx.tsv 5
echo 'dg92-zbpx.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/dg92-zbpx.tsv.avfout AVF/dg92-zbpx.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/cyfw-hfqk.tsv.avfout
echo 'cyfw-hfqk.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/cyfw-hfqk.tsv 5
echo 'cyfw-hfqk.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/cyfw-hfqk.tsv.avfout AVF/cyfw-hfqk.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/c72z-kzbi.tsv.avfout
echo 'c72z-kzbi.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/c72z-kzbi.tsv 5
echo 'c72z-kzbi.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/c72z-kzbi.tsv.avfout AVF/c72z-kzbi.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/bawj-6bgn.tsv.avfout
echo 'bawj-6bgn.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/bawj-6bgn.tsv 5
echo 'bawj-6bgn.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/bawj-6bgn.tsv.avfout AVF/bawj-6bgn.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ayeb-p4mv.tsv.avfout
echo 'ayeb-p4mv.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ayeb-p4mv.tsv 5
echo 'ayeb-p4mv.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ayeb-p4mv.tsv.avfout AVF/ayeb-p4mv.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/ay9k-vznm.tsv.avfout
echo 'ay9k-vznm.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/ay9k-vznm.tsv 5
echo 'ay9k-vznm.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/ay9k-vznm.tsv.avfout AVF/ay9k-vznm.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/799n-b76v.tsv.avfout
echo '799n-b76v.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/799n-b76v.tsv 5
echo '799n-b76v.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/799n-b76v.tsv.avfout AVF/799n-b76v.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/735p-zed8.tsv.avfout
echo '735p-zed8.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/735p-zed8.tsv 5
echo '735p-zed8.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/735p-zed8.tsv.avfout AVF/735p-zed8.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/56bx-u7iw.tsv.avfout
echo '56bx-u7iw.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/56bx-u7iw.tsv 5
echo '56bx-u7iw.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/56bx-u7iw.tsv.avfout AVF/56bx-u7iw.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/42et-jh9v.tsv.avfout
echo '42et-jh9v.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/42et-jh9v.tsv 5
echo '42et-jh9v.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/42et-jh9v.tsv.avfout AVF/42et-jh9v.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/35cc-g4mc.tsv.avfout
echo '35cc-g4mc.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/35cc-g4mc.tsv 5
echo '35cc-g4mc.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/35cc-g4mc.tsv.avfout AVF/35cc-g4mc.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/29bw-z7pj.tsv.avfout
echo '29bw-z7pj.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/29bw-z7pj.tsv 5
echo '29bw-z7pj.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/29bw-z7pj.tsv.avfout AVF/29bw-z7pj.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/8vgb-zm6e.tsv.avfout
echo '8vgb-zm6e.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/8vgb-zm6e.tsv 5
echo '8vgb-zm6e.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/8vgb-zm6e.tsv.avfout AVF/8vgb-zm6e.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/8isn-pgv3.tsv.avfout
echo '8isn-pgv3.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/8isn-pgv3.tsv 5
echo '8isn-pgv3.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/8isn-pgv3.tsv.avfout AVF/8isn-pgv3.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/8fnh-fcum.tsv.avfout
echo '8fnh-fcum.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/8fnh-fcum.tsv 5
echo '8fnh-fcum.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/8fnh-fcum.tsv.avfout AVF/8fnh-fcum.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/7ujc-hpzg.tsv.avfout
echo '7ujc-hpzg.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/7ujc-hpzg.tsv 5
echo '7ujc-hpzg.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/7ujc-hpzg.tsv.avfout AVF/7ujc-hpzg.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/7kc8-z939.tsv.avfout
echo '7kc8-z939.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/7kc8-z939.tsv 5
echo '7kc8-z939.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/7kc8-z939.tsv.avfout AVF/7kc8-z939.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/7dfh-3irt.tsv.avfout
echo '7dfh-3irt.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/7dfh-3irt.tsv 5
echo '7dfh-3irt.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/7dfh-3irt.tsv.avfout AVF/7dfh-3irt.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/6xp4-c9qp.tsv.avfout
echo '6xp4-c9qp.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/6xp4-c9qp.tsv 5
echo '6xp4-c9qp.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/6xp4-c9qp.tsv.avfout AVF/6xp4-c9qp.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/6rrm-vxj9.tsv.avfout
echo '6rrm-vxj9.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/6rrm-vxj9.tsv 5
echo '6rrm-vxj9.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/6rrm-vxj9.tsv.avfout AVF/6rrm-vxj9.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/6jad-5sav.tsv.avfout
echo '6jad-5sav.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/6jad-5sav.tsv 5
echo '6jad-5sav.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/6jad-5sav.tsv.avfout AVF/6jad-5sav.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/6bic-qvek.tsv.avfout
echo '6bic-qvek.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/6bic-qvek.tsv 5
echo '6bic-qvek.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/6bic-qvek.tsv.avfout AVF/6bic-qvek.tsv.avfout

/usr/bin/hadoop fs -rm -r GROUP2/5uac-w243.tsv.avfout
echo '5uac-w243.tsv Start:' $(date) >> AVF/time.log
spark-submit --conf spark.pyspark.python=/share/apps/python/3.4.4/bin/python a3.py GROUP2/5uac-w243.tsv 5
echo '5uac-w243.tsv End:' $(date) >> AVF/time.log
/usr/bin/hadoop fs -getmerge GROUP2/5uac-w243.tsv.avfout AVF/5uac-w243.tsv.avfout
