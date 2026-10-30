"""Add durable input while preserving pre-existing job rows."""

from alembic import op
import sqlalchemy as sa

revision: str = '0002_processing_input'
down_revision: str = '0001_processing_jobs'
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # No backfill: only legacy rows may lack the complete input. The repository
    # requires validated metadata for every new acceptance. A future revision
    # may set NOT NULL only after an explicit legacy-data regularization plan.
    op.add_column('processing_jobs', sa.Column('storage_key', sa.Text()))
    op.add_column('processing_jobs', sa.Column('media_type', sa.Text()))
    op.add_column('processing_jobs', sa.Column('size_bytes', sa.Numeric()))
    op.add_column('processing_jobs', sa.Column('sha256', sa.String(64)))
    op.create_check_constraint(
        'ck_processing_jobs_input_complete', 'processing_jobs',
        'num_nonnulls(storage_key, media_type, size_bytes, sha256) IN (0, 4)',
    )
    op.create_check_constraint(
        'ck_processing_jobs_input_size', 'processing_jobs',
        'size_bytes >= 0 AND size_bytes = trunc(size_bytes) '
        "AND size_bytes NOT IN ('NaN'::numeric, 'Infinity'::numeric, "
        "'-Infinity'::numeric)",
    )
    op.create_check_constraint(
        'ck_processing_jobs_input_sha256', 'processing_jobs',
        "sha256 ~ '^[0-9a-f]{64}$'",
    )


def downgrade() -> None:
    # Destructive for input metadata; run only on disposable databases or after
    # an explicitly approved backup/data-retention decision.
    for name in (
        'ck_processing_jobs_input_sha256',
        'ck_processing_jobs_input_size',
        'ck_processing_jobs_input_complete',
    ):
        op.drop_constraint(name, 'processing_jobs', type_='check')
    for name in ('sha256', 'size_bytes', 'media_type', 'storage_key'):
        op.drop_column('processing_jobs', name)
