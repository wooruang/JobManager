import datetime


class JobContext:
    name: str  # Job 이름
    category: str  # Job 분류
    job_type: str  # Job 타입
    state: str  # 현재 상태 [('READY', 'Ready'), ('GENERATING', 'Generating'), ('DONE', 'Done'), ('ERROR', 'Error')]
    all_stage: dict  # Job에 해당하는 모든 stage
    condition: dict  # Job 실행 조건
    context: dict  # Job 실행 데이터
    reason: str  # Job 상태 이유
    from_job: int  # Job 생성 주체
    last_generated_at: datetime.datetime  # 최근 발생일
    created_at: datetime.datetime  # 생성일
    updated_at: datetime.datetime  # 수정일
