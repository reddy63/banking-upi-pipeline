from pathlib import Path

f = Path('scripts/check_pipeline.py')
lines = f.read_text().splitlines()

# Find and completely rewrite the task check block
start_idx = None
end_idx = None
for i, line in enumerate(lines):
    if "for task in ['ingest_csv'" in line:
        start_idx = i
    if start_idx and i > start_idx and 'fail(f"DAG task' in line:
        end_idx = i
        break

if start_idx and end_idx:
    new_block = [
        "for task in ['ingest_csv','ingest_api','raw_manifest','snowflake_ingest',",
        "             'dbt_run_prep','dbt_snapshot','dbt_run_marts','dbt_test','pipeline_summary']:",
        "    found = any(f'task_id{sp}={sp}\"{task}"' in dag_src for sp in ['', ' ', '         '])",
        "    if found:",
        "        ok(f'DAG task: {task}')",
        "    else:",
        "        fail(f'DAG task: {task}', 'task_id not found')",
    ]
    lines[start_idx:end_idx+1] = new_block
    print(f'Rewrote task check block lines {start_idx}-{end_idx}')
else:
    print(f'Block not found: start={start_idx} end={end_idx}')

f.write_text('\n'.join(lines))
print('Done')
