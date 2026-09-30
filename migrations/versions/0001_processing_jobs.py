"""Create the Processing jobs table."""

from typing import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = '0001_processing_jobs'
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        'processing_jobs',
        sa.Column('processing_id', sa.Uuid(as_uuid=True), nullable=False),
        sa.Column('source_event_id', sa.String(), nullable=False),
        sa.Column('document_id', sa.String(), nullable=False),
        sa.Column('correlation_id', sa.String(), nullable=True),
        sa.Column('status', sa.String(length=32), nullable=False),
        sa.Column('attempts', sa.Integer(), nullable=False),
        sa.Column(
            'created_at', sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            'updated_at', sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column('started_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('finished_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('failure_reason', sa.String(), nullable=True),
        sa.CheckConstraint(
            'attempts >= 0',
            name='ck_processing_jobs_attempts',
        ),
        sa.PrimaryKeyConstraint('processing_id'),
        sa.UniqueConstraint(
            'source_event_id',
            name='uq_processing_jobs_source_event_id',
        ),
    )


def downgrade() -> None:
    op.drop_table('processing_jobs')
