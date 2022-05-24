spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 4 \
    Ass2.py \
    --output $1
