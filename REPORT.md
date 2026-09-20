# Báo cáo Tutorial 03 — Airflow

Toàn bộ 4 bài tập trong `README.md` đã được chạy và kiểm chứng. Log gốc của từng lần
chạy nằm trong thư mục [`evidence/`](evidence/).

## Môi trường

Chạy theo **cách B (Docker)** trong README, vì máy chỉ có Python 3.9 trong khi
Airflow 2.8.4 của tutorial cần Python 3.11:

| | |
|---|---|
| Image | `ddm501-t03-airflow:2.8.4` (build từ `Dockerfile`) |
| Airflow | 2.8.4, Python 3.11, `SequentialExecutor`, SQLite |
| pandas / pyarrow / numpy | 2.1.4 / 14.0.2 / 1.24.4 |
| Web UI | <http://127.0.0.1:18080> — scheduler, triggerer, metadatabase đều `healthy` |
| DAG | `wdbc_pipeline` nạp không lỗi import |
| Dữ liệu vào | `data/raw/wdbc.csv` — 570 dòng, 32 cột |

Chạy lại từ đầu:

```bash
docker compose up -d --build
docker compose exec airflow airflow dags test wdbc_pipeline 2026-08-25
```

## Bài 1 — Chạy lại một ngày là an toàn

Chạy `airflow dags test wdbc_pipeline 2026-08-25` hai lần.

Cả hai lần đều thành công cả 5 task (`ingest → validate → split → scale → report`):
570 dòng vào, 6 dòng bị loại (1.05%), còn 564 dòng sạch, chia 441 train / 123 test,
scaler fit trên 441 dòng train.

**Kết quả kiểm chứng:** so sánh SHA-256 của toàn bộ 10 file đầu ra sau mỗi lần chạy
cho kết quả **giống nhau từng byte**, và `history.jsonl` **vẫn chỉ có 1 dòng** cho
ngày `2026-08-25`.

```
$ diff evidence/ex1-checksums-run1.txt evidence/ex1-checksums-run2.txt
(không có khác biệt)
```

Hai lý do khiến việc này đúng, đọc được trong `dags/wdbc_pipeline.py`:

- `split` chia train/test bằng cách **hash `sample_id`** (`sha256 % 100 < 20`) chứ
  không dùng random seed, nên một dòng luôn rơi vào cùng một phía, trên mọi máy.
- `report` **lọc bỏ dòng cũ cùng `ds`** trước khi ghi lại `history.jsonl`, nên chạy
  lại một ngày không sinh thêm dòng trùng.

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

**Điểm đáng chú ý:** DAG khai báo `retries: 3`, tức là task được phép chạy tới 4 lần.
Nhưng log của lần chạy này ghi `Starting attempt 1 of 4` rồi dừng luôn ở
`Immediate failure requested`, và trên đĩa chỉ có duy nhất `attempt=1.log`. Đó là tác
dụng của `AirflowFailException`: một file hỏng thì thử lại lần thứ tư vẫn hỏng, nên
retry bị bỏ qua có chủ đích. Ba task phía sau chuyển thành `upstream_failed` chứ không
chạy trên dữ liệu rác.

Sau `python scripts/corrupt_extract.py --repair`, `wdbc.csv` khớp SHA-256 với bản
backup, chạy lại `2026-08-25` thì thành công và đầu ra **giống từng byte** với bài 1.

Bằng chứng: `ex2-corrupt.log`, `ex2-run-failed.log`, `ex2-task-states.txt`,
`ex2-repair.log`, `ex2-run-after-repair.log`, `ex2-checksums-after-repair.txt`.

## Bài 3 — Backfill ba ngày

```bash
airflow dags backfill wdbc_pipeline -s 2026-08-22 -e 2026-08-24
```

Kết thúc với `finished run 3 of 3 | succeeded: 15 | failed: 0`. Sinh ra đúng ba thư mục
`data/staging/2026-08-22`, `2026-08-23`, `2026-08-24`, và `history.jsonl` có thêm ba
dòng (tổng 4 dòng, kể cả `2026-08-25` từ bài 1):

```json
{"ds": "2026-08-25", "clean_rows": 564, "train": 441, "test": 123, ...}
{"ds": "2026-08-22", "clean_rows": 564, "train": 441, "test": 123, ...}
{"ds": "2026-08-23", "clean_rows": 564, "train": 441, "test": 123, ...}
{"ds": "2026-08-24", "clean_rows": 564, "train": 441, "test": 123, ...}
```

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
của đúng một task, đúng một ngày, không cần SSH vào đâu — nội dung này được lưu lại ở
`ex4-validate-task-log.txt`.

**Một điều phát hiện khi làm bài này:** `airflow dags test` chỉ in log ra terminal,
**không** ghi file log mà UI đọc. Vì vậy lần lỗi tạo bằng `dags test` ở bài 2 hiện
trong Grid view nhưng tab Logs lại báo `Could not read served logs: 404`. Để bài 4 có
log thật sự đọc được trên UI, lần lỗi ở `2026-08-26` được tạo bằng `dags backfill` trên
file đã làm hỏng — backfill chạy task qua executor nên có ghi
`logs/dag_id=wdbc_pipeline/run_id=backfill__2026-08-26.../task_id=validate/attempt=1.log`.
Dùng ngày `2026-08-26` riêng để bốn ngày của bài 1 và bài 3 vẫn xanh.

Sau khi tạo xong lần lỗi đó, `wdbc.csv` đã được `--repair` về nguyên trạng.

Bằng chứng: `ex4-ui-health.json`, `ex4-ui-grid-data.txt`, `ex4-validate-task-log.txt`,
`ex4-backfill-failed.log`.

## Thay đổi so với repo gốc

Chỉ một thay đổi về cấu hình, trong `docker-compose.yml`:

```yaml
- ./scripts:/opt/airflow/scripts
```

`docker-compose.yml` vốn chỉ mount `dags`, `data`, `logs`. Bài 2 và bài 4 cần chạy
`scripts/corrupt_extract.py` nhắm vào `data/raw/wdbc.csv` đang được mount, nên khi chạy
theo cách Docker thì `scripts/` cũng phải nằm trong container. Không sửa gì trong
`dags/wdbc_pipeline.py` hay `scripts/corrupt_extract.py`.

Lưu ý: `.gitignore` loại `data/staging/` và `logs/` khỏi repo, nên đầu ra của pipeline
không được commit — đó là lý do các bằng chứng được gom vào `evidence/`.
