@echo off
echo Starting Jupyter inside spark-master...

docker exec -it spark-master jupyter notebook ^
  --ip=0.0.0.0 ^
  --no-browser ^
  --allow-root ^
  --port=8888

pause