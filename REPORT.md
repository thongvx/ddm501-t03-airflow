# Báo cáo Tutorial 03 — Airflow

Toàn bộ 4 bài tập trong `README.md` đã được chạy và kiểm chứng, cộng thêm một phần mở
rộng: task `train` ghi metric vào MLflow. Log gốc của từng lần chạy nằm trong thư mục
[`evidence/`](evidence/).

## Môi trường

Chạy theo **cách B (Docker)** trong README, vì máy chỉ có Python 3.9 trong khi
Airflow 2.8.4 của tutorial cần Python 3.11:

| | |
|---|---|
| Airflow | 2.8.4, Python 3.11, `SequentialExecutor`, SQLite |
| MLflow | 2.19.0 — server riêng một container, backend SQLite |
| pandas / pyarrow / numpy | 2.1.4 / 14.0.2 / 1.24.4 |
| scikit-learn | 1.6.0 |
| Airflow UI | <http://127.0.0.1:18080> — scheduler, triggerer, metadatabase đều `healthy` |
| MLflow UI | <http://127.0.0.1:15030> |
| DAG | `wdbc_pipeline`, 6 task, nạp không lỗi import |
| Dữ liệu vào | `data/raw/wdbc.csv` — 570 dòng, 32 cột |

Chạy lại từ đầu:

```bash
docker compose up -d --build
docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25
```

## Bài 1 — Chạy lại một ngày là an toàn

Chạy `airflow dags test wdbc_pipeline 2026-08-25` hai lần.

Cả hai lần đều thành công cả 6 task (`ingest → validate → split → scale → train →
report`): 570 dòng vào, 6 dòng bị loại (1.05%), còn 564 dòng sạch, chia 441 train /
123 test, scaler fit trên 441 dòng train.

**Kết quả kiểm chứng:** so sánh SHA-256 của toàn bộ file đầu ra sau mỗi lần chạy cho
kết quả **giống nhau từng byte** (kể cả `metrics.json` mới), `history.jsonl` **vẫn chỉ
có 1 dòng** cho ngày `2026-08-25`, và MLflow **vẫn chỉ có 1 run** cho ngày đó.

```
$ diff evidence/ex1-checksums-run1.txt evidence/ex1-checksums-run2.txt
(không có khác biệt)

MLflow sau run1: 1 run(s): ['2026-08-25']
MLflow sau run2: 1 run(s): ['2026-08-25']
```

Ba lý do khiến việc này đúng:

- `split` chia train/test bằng cách **hash `sample_id`** (`sha256 % 100 < 20`) chứ
  không dùng random seed, nên một dòng luôn rơi vào cùng một phía, trên mọi máy.
- `report` **lọc bỏ dòng cũ cùng `ds`** trước khi ghi lại `history.jsonl`.
- `train` **xoá run MLflow cũ có cùng tag `ds`** trước khi ghi run mới, đúng quy tắc
  `report` áp cho `history.jsonl`. Nếu không làm vậy thì chạy lại một ngày sẽ để lại
  hai run mâu thuẫn nhau trong MLflow.

Bằng chứng: `ex1-run1.log`, `ex1-run2.log`, `ex1-checksums-run1.txt`, `ex1-checksums-run2.txt`.

## Bài 2 — Pipeline từ chối dữ liệu hỏng, và không đốt retry

`python scripts/corrupt_extract.py` xoá trắng `mean_radius` trên 68/570 dòng (11.9%).
Chạy lại `2026-08-25`, task `validate` thất bại đúng như README dự đoán:

```
validation: {'null': 70, 'negative': 1, 'bad_label': 1, 'duplicate': 1, 'outlier': 1}  (12.98% of rows rejected)
airflow.exceptions.AirflowFailException: 13.0% of rows rejected, limit is 5%
Immediate failure requested. Marking task as FAILED.
```

Con số 13.0% là 68 dòng bị làm hỏng cộng với 6 dòng vốn đã xấu trong dữ liệu gốc.

**Retry bị bỏ qua có chủ đích.** DAG khai báo `retries: 3`, tức task được phép chạy tới
4 lần. Nhưng log ghi `Starting attempt 1 of 4` rồi dừng luôn ở `Immediate failure
requested`, và trên đĩa chỉ có duy nhất `attempt=1.log`. Đó là tác dụng của
`AirflowFailException`: một file hỏng thì thử lần thứ tư vẫn hỏng.

**Dữ liệu rác không bao giờ tới được model.** Vì `train` nằm sau `validate`, nó dừng ở
trạng thái `upstream_failed` cùng với `split`, `scale`, `report`. Không có model nào
được train trên 13% dòng bị loại, và MLflow không có run nào cho ngày đó:

```
| validate | failed          |
| split    | upstream_failed |
| scale    | upstream_failed |
| train    | upstream_failed |
| report   | upstream_failed |
```

Sau `--repair`, `wdbc.csv` khớp SHA-256 với bản backup, chạy lại `2026-08-25` thì thành
công và đầu ra **giống từng byte** với bài 1.

Bằng chứng: `ex2-corrupt.log`, `ex2-run-failed.log`, `ex2-task-states.txt`,
`ex2-repair.log`, `ex2-run-after-repair.log`, `ex2-checksums-after-repair.txt`.

## Bài 3 — Backfill ba ngày

```bash
airflow dags backfill wdbc_pipeline -s 2026-08-22 -e 2026-08-24
```

Kết thúc với `finished run 3 of 3 | succeeded: 18 | failed: 0` — 18 task, tức 6 task
nhân 3 ngày. Sinh ra đúng ba thư mục `data/staging/2026-08-22`, `2026-08-23`,
`2026-08-24`, `history.jsonl` có thêm ba dòng (tổng 4 dòng, kể cả `2026-08-25` từ bài
1), và MLflow có 4 run, mỗi ngày một run.

Các con số giống nhau giữa bốn ngày là hợp lý: `ingest` đọc cùng một file nguồn tĩnh,
`ds` chỉ quyết định thư mục đầu ra. Điểm cần thấy ở đây là **mỗi ngày có thư mục riêng,
không ngày nào ghi đè ngày nào**.

Bằng chứng: `ex3-backfill.log`.

## Bài 4 — Đọc traceback của một task lỗi trên UI

Đăng nhập UI tại <http://127.0.0.1:18080> (user `admin`, mật khẩu lấy bằng
`docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt`).

Grid view hiện 5 lần chạy, trong đó có một lần đỏ để xem traceback:

```
backfill__2026-08-22  -> success
backfill__2026-08-23  -> success
backfill__2026-08-24  -> success
manual__2026-08-25    -> success
backfill__2026-08-26  -> failed
```

Bấm vào task `validate` đỏ của `2026-08-26` rồi mở tab **Logs** cho ra traceback đầy đủ
của đúng một task, đúng một ngày, không cần SSH vào đâu.

**Một điều phát hiện khi làm bài này:** `airflow dags test` chỉ in log ra terminal,
**không** ghi file log mà UI đọc. Vì vậy lần lỗi tạo bằng `dags test` ở bài 2 hiện
trong Grid view nhưng tab Logs lại báo `Could not read served logs: 404`. Để bài 4 có
log thật sự đọc được trên UI, lần lỗi ở `2026-08-26` được tạo bằng `dags backfill` trên
file đã làm hỏng — backfill chạy task qua executor nên có ghi file log. Dùng ngày
`2026-08-26` riêng để bốn ngày của bài 1 và bài 3 vẫn xanh.

Bằng chứng: `ex4-ui-health.json`, `ex4-ui-grid-data.txt`, `ex4-validate-task-log.txt`,
`ex4-task-states.txt`, `ex4-backfill-failed.log`.

## Phần mở rộng — task `train` ghi metric vào MLflow

`train` nằm giữa `scale` và `report`: fit logistic regression trên tập train đã scale,
đánh giá trên tập test, rồi ghi params, metrics và cả model vào MLflow ở experiment
`wdbc_pipeline`, gắn tag `ds`.

Kết quả trên tập test 123 dòng (nhãn dương là `M` — khối u ác tính, lớp đáng bắt):

| accuracy | precision | recall | f1 | roc_auc |
|---|---|---|---|---|
| 0.95122 | 0.959184 | 0.921569 | 0.94 | 0.995643 |

Model được log đầy đủ nên tải lại được, không chỉ có metric:

```
model/MLmodel  model/model.pkl  model/conda.yaml  model/python_env.yaml  model/requirements.txt
```

Ba quyết định thiết kế đáng nói:

**Train trên file đã scale, không phải file thô.** `scale` fit scaler chỉ trên tập
train, nên `train.parquet` / `test.parquet` là cặp duy nhất không bị rò rỉ thông tin
của tập test vào bước chuẩn hoá.

**Giữ được tính idempotent của bài 1.** `metrics.json` làm tròn 6 chữ số thập phân để
chạy lại một ngày ghi ra đúng cùng bytes. MLflow run id **không** được đưa vào
`summary.json` — nó đổi mỗi lần chạy nên sẽ phá vỡ bài 1. Run id chỉ xuất hiện trong
log của task và trong XCom; muốn tìm run của một ngày thì lọc theo tag `ds`.

**Import nặng nằm trong task, không nằm ở đầu file.** Scheduler parse lại mọi file
trong thư mục DAG liên tục và không cần đến sklearn; tiến trình task import một lần,
đúng lúc cần.

Bằng chứng: `ex5-mlflow-runs.txt`, `ex5-mlflow-artifacts.txt`.

## Thứ tự khởi động và tính tương thích Linux

**MLflow không phải chờ service nào.** Khác Lab 2 (MLflow ở đó phụ thuộc Postgres và
MinIO), server ở đây dùng SQLite trong một volume của chính nó, nên không có phụ thuộc
nào để chờ. Chiều phụ thuộc là chiều còn lại: **Airflow phải chờ MLflow**, và nó chờ
tới trạng thái `healthy` chứ không chỉ `started`:

```
Container ddm501-t03-mlflow Starting
Container ddm501-t03-mlflow Started
Container ddm501-t03-mlflow Waiting     <- compose dừng ở đây
Container ddm501-t03-mlflow Healthy     <- tới khi healthcheck xanh
Container ddm501-t03-airflow Starting   <- rồi mới khởi động Airflow
```

`depends_on` chỉ chi phối lúc khởi động. Nếu MLflow chết giữa lúc pipeline đang chạy
thì `train` sẽ lỗi kết nối — và vì đó là exception thường chứ không phải
`AirflowFailException`, nó được retry 3 lần với exponential backoff theo `default_args`.
Đây là đúng hành vi mong muốn: sự cố tạm thời thì thử lại, dữ liệu hỏng thì bỏ ngay.

Mục tiêu của phần này: `git clone` về VPS rồi `docker compose up -d` là chạy được
ngay, không có bước thủ công nào để quên. Ban đầu repo **không** đạt mục tiêu đó, và
lý do không thấy được trên macOS — Docker Desktop tự map quyền sở hữu bind mount nên
nó che mất cả hai lỗi. Ba thứ phải sửa:

1. **`.gitkeep` của `logs/` và `mlflow-data/` phải được commit.** `.gitignore` đang
   ignore cả thư mục `logs/`, nên `logs/.gitkeep` chưa từng vào index và bản clone
   không có `logs/`. Khi thư mục nguồn của bind mount không tồn tại, Docker tự tạo nó
   với quyền root; container chạy UID thường thì không ghi được, và scheduler đổ ngay
   ở file log đầu tiên. Đã đổi thành `logs/*` + `!logs/.gitkeep` cho cả hai thư mục.
2. **Mặc định phải là root, không phải 50000.** UID 50000 là user riêng của image
   Airflow, trên VPS nó không sở hữu gì cả, nên dù thư mục đã tồn tại thì container
   vẫn không tạo được `data/staging/`. Giờ `user: "${AIRFLOW_UID:-0}:0"` — root ghi
   được bind mount bất kể UID của host. Giá phải trả là file sinh ra thuộc root; ai
   cần lấy lại quyền thì `cp .env.example .env` và điền UID của mình, còn bản thân
   các thư mục thì đã thuộc quyền người clone nhờ điểm 1.
3. **`HOME=/home/airflow` trong compose.** Airflow trong image được cài bằng
   `pip install --user`, nên Python chỉ tìm thấy nó dưới home của UID đang chạy.
   Entrypoint lo việc đó cho tiến trình chính của container, nhưng
   `docker compose exec` **không** đi qua entrypoint — thiếu biến này thì mọi lệnh
   trong bài tập chết với `No module named 'airflow'` ngay khi UID khác 50000. Đây là
   hệ quả trực tiếp của điểm 2 và chỉ lộ ra khi chạy thật.

Không dùng cú pháp riêng của Docker Desktop: không `host.docker.internal`, không
`:cached` / `:delegated`, không ghim `platform:`.

Kiểm chứng bằng cách `git clone` repo ra thư mục khác rồi chạy từ đó, **không** tạo
`.env`: stack lên `healthy`, pipeline chạy thành công, ghi đủ `data/staging/`, log ra
host và run vào MLflow. Chạy lại nhánh có `.env` (UID thật) cũng được, và vẫn
idempotent. Vẫn phải nói rõ giới hạn: các lần chạy này đều từ macOS, nên chúng chứng
minh *cấu hình* đúng chứ không thay được một lần chạy thật trên Linux.

Bằng chứng: `ex6-linux-portability.txt`, `ex7-fresh-clone.txt`, `startup-order.log`.

## Thay đổi so với repo gốc

| File | Thay đổi | Vì sao |
|---|---|---|
| `docker-compose.yml` | mount thêm `./scripts` | Bài 2 và 4 cần chạy `corrupt_extract.py` nhắm vào extract đang được mount; compose gốc chỉ mount `dags`, `data`, `logs` nên hai bài này không chạy được theo cách Docker |
| `docker-compose.yml` | thêm service `mlflow` (port 15030, bind mount `./mlflow-data`), `depends_on: service_healthy`, biến `MLFLOW_TRACKING_URI`, `GIT_PYTHON_REFRESH=quiet` | Phần mở rộng MLflow. Bind mount thay vì named volume để run còn lại trên đĩa và đọc được, theo cách Lab 2 làm |
| `docker-compose.yml` | mặc định `user` thành root, thêm `HOME=/home/airflow` | Để clone về VPS là chạy được ngay — xem phần tương thích Linux ở trên |
| `Dockerfile` | thêm `mlflow-skinny==2.19.0`, `scikit-learn==1.6.0`, chạy `pip check` | Client tracking cho task `train`. Dùng `mlflow-skinny` vì `mlflow` đầy đủ kéo theo Flask/SQLAlchemy/alembic riêng, xung đột với version Airflow 2.8.4 ghim. `pip check` cho build đổ ngay nếu sau này có xung đột |
| `Dockerfile.mlflow` | file mới | Server MLflow ở image riêng, không tranh dependency với Airflow |
| `dags/wdbc_pipeline.py` | thêm task `train`, `report` nhận thêm tham số | Phần mở rộng MLflow |
| `requirements.txt` | thêm 2 pin trên | Cho cách chạy venv local |
| `.gitignore`, `.dockerignore` | thêm `mlruns/`, `.DS_Store`; `logs/` và `mlflow-data/` đổi sang ignore phần nội dung nhưng giữ `.gitkeep` | `mlruns/` là nơi client MLflow ghi nếu ai đó trỏ `MLFLOW_TRACKING_URI` về thư mục. `.gitkeep` phải được commit, xem phần tương thích Linux |
| `.env.example` | file mới | Tuỳ chọn, chỉ để đổi UID cho file sinh ra thuộc quyền mình thay vì root |

Port 15030 theo quy ước của Lab 2 (MLflow ở đó dùng 15020) và tránh 5000 vì macOS dành
port đó cho AirPlay receiver.

Lưu ý: `.gitignore` loại `data/staging/` và `logs/` khỏi repo, nên đầu ra của pipeline
không được commit — đó là lý do các bằng chứng được gom vào `evidence/`.
