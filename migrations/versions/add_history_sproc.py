"""add GetReleaseItemHistoryById stored procedure with active tracking

Revision ID: add_history_sproc
Revises: add_active_col
Create Date: 2026-03-24 17:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'add_history_sproc'
down_revision = 'add_active_col'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("""
    CREATE OR ALTER PROCEDURE [dbo].[GetReleaseItemHistoryById]
        @ReleaseItemId VARCHAR(50)
    AS
    BEGIN
    WITH Hist AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY release_item_id ORDER BY ValidFrom) AS VersionNum
        FROM dbo.release_items FOR SYSTEM_TIME ALL
        WHERE release_item_id = @ReleaseItemId
    ),
    Diffs AS (
        SELECT
            h.*,
            LAG(release_date)        OVER (PARTITION BY release_item_id ORDER BY VersionNum) AS p_release_date,
            LAG(release_type)        OVER (PARTITION BY release_item_id ORDER BY VersionNum) AS p_release_type,
            LAG(release_status)      OVER (PARTITION BY release_item_id ORDER BY VersionNum) AS p_release_status,
            LAG(feature_description) OVER (PARTITION BY release_item_id ORDER BY VersionNum) AS p_feature_description,
            LAG(active)              OVER (PARTITION BY release_item_id ORDER BY VersionNum) AS p_active
        FROM Hist h
    )
    SELECT
        d.VersionNum,
        d.release_item_id,
        ChangedColumns =
            CASE
                WHEN d.VersionNum = 1 THEN 'Roadmap Item Added'
                ELSE (
                    SELECT STRING_AGG(v.ColName, ',') WITHIN GROUP (ORDER BY v.ColOrder)
                    FROM (
                        -- 1: Release Date
                        SELECT 1 AS ColOrder,
                               CASE WHEN (d.release_date <> d.p_release_date)
                                     OR (d.release_date IS NULL AND d.p_release_date IS NOT NULL)
                                     OR (d.release_date IS NOT NULL AND d.p_release_date IS NULL)
                                    THEN CONCAT('Release Date ',
                                                COALESCE(CONVERT(varchar(30), d.p_release_date, 23), '(null)'),
                                                ' -> ',
                                                COALESCE(CONVERT(varchar(30), d.release_date, 23), '(null)'))
                               END AS ColName
                        UNION ALL
                        -- 2: Release Type
                        SELECT 2,
                               CASE WHEN (d.release_type <> d.p_release_type)
                                     OR (d.release_type IS NULL AND d.p_release_type IS NOT NULL)
                                     OR (d.release_type IS NOT NULL AND d.p_release_type IS NULL)
                                    THEN CONCAT('Release Type ',
                                                COALESCE(d.p_release_type, '(null)'),
                                                ' -> ',
                                                COALESCE(d.release_type, '(null)'))
                               END
                        UNION ALL
                        -- 3: Release Status
                        SELECT 3,
                               CASE WHEN (d.release_status <> d.p_release_status)
                                     OR (d.release_status IS NULL AND d.p_release_status IS NOT NULL)
                                     OR (d.release_status IS NOT NULL AND d.p_release_status IS NULL)
                                    THEN CONCAT('Release Status ',
                                                COALESCE(d.p_release_status, '(null)'),
                                                ' -> ',
                                                COALESCE(d.release_status, '(null)'))
                               END
                        UNION ALL
                        -- 4: Feature Description (trim or shorten if needed)
                        SELECT 4,
                               CASE WHEN (d.feature_description <> d.p_feature_description)
                                     OR (d.feature_description IS NULL AND d.p_feature_description IS NOT NULL)
                                     OR (d.feature_description IS NOT NULL AND d.p_feature_description IS NULL)
                                    THEN CONCAT('Feature Description ',
                                                LEFT(COALESCE(d.p_feature_description, '(null)'), 4000),
                                                ' -> ',
                                                LEFT(COALESCE(d.feature_description, '(null)'), 4000))
                               END
                        UNION ALL
                        -- 5: Active / Removed status
                        SELECT 5,
                               CASE WHEN (d.active <> d.p_active)
                                     OR (d.active IS NULL AND d.p_active IS NOT NULL)
                                     OR (d.active IS NOT NULL AND d.p_active IS NULL)
                                    THEN CASE
                                        WHEN d.active = 0 THEN 'Removed from Roadmap'
                                        WHEN d.active = 1 THEN 'Restored to Roadmap'
                                        ELSE CONCAT('Active ',
                                                    COALESCE(CAST(d.p_active AS varchar), '(null)'),
                                                    ' -> ',
                                                    COALESCE(CAST(d.active AS varchar), '(null)'))
                                    END
                               END
                    ) v
                    WHERE v.ColName IS NOT NULL
                )
            END,
        d.last_modified
    FROM Diffs d
    WHERE CASE
                WHEN d.VersionNum = 1 THEN 'Roadmap Item Added'
                ELSE (
                    SELECT STRING_AGG(v.ColName, ',') WITHIN GROUP (ORDER BY v.ColOrder)
                    FROM (
                        -- 1: Release Date
                        SELECT 1 AS ColOrder,
                               CASE WHEN (d.release_date <> d.p_release_date)
                                     OR (d.release_date IS NULL AND d.p_release_date IS NOT NULL)
                                     OR (d.release_date IS NOT NULL AND d.p_release_date IS NULL)
                                    THEN CONCAT('Release Date ',
                                                COALESCE(CONVERT(varchar(30), d.p_release_date, 23), '(null)'),
                                                ' -> ',
                                                COALESCE(CONVERT(varchar(30), d.release_date, 23), '(null)'))
                               END AS ColName
                        UNION ALL
                        -- 2: Release Type
                        SELECT 2,
                               CASE WHEN (d.release_type <> d.p_release_type)
                                     OR (d.release_type IS NULL AND d.p_release_type IS NOT NULL)
                                     OR (d.release_type IS NOT NULL AND d.p_release_type IS NULL)
                                    THEN CONCAT('Release Type ',
                                                COALESCE(d.p_release_type, '(null)'),
                                                ' -> ',
                                                COALESCE(d.release_type, '(null)'))
                               END
                        UNION ALL
                        -- 3: Release Status
                        SELECT 3,
                               CASE WHEN (d.release_status <> d.p_release_status)
                                     OR (d.release_status IS NULL AND d.p_release_status IS NOT NULL)
                                     OR (d.release_status IS NOT NULL AND d.p_release_status IS NULL)
                                    THEN CONCAT('Release Status ',
                                                COALESCE(d.p_release_status, '(null)'),
                                                ' -> ',
                                                COALESCE(d.release_status, '(null)'))
                               END
                        UNION ALL
                        -- 4: Feature Description (trim or shorten if needed)
                        SELECT 4,
                               CASE WHEN (d.feature_description <> d.p_feature_description)
                                     OR (d.feature_description IS NULL AND d.p_feature_description IS NOT NULL)
                                     OR (d.feature_description IS NOT NULL AND d.p_feature_description IS NULL)
                                    THEN CONCAT('Feature Description ',
                                                LEFT(COALESCE(d.p_feature_description, '(null)'), 4000),
                                                ' -> ',
                                                LEFT(COALESCE(d.feature_description, '(null)'), 4000))
                               END
                        UNION ALL
                        -- 5: Active / Removed status
                        SELECT 5,
                               CASE WHEN (d.active <> d.p_active)
                                     OR (d.active IS NULL AND d.p_active IS NOT NULL)
                                     OR (d.active IS NOT NULL AND d.p_active IS NULL)
                                    THEN CASE
                                        WHEN d.active = 0 THEN 'Removed from Roadmap'
                                        WHEN d.active = 1 THEN 'Restored to Roadmap'
                                        ELSE CONCAT('Active ',
                                                    COALESCE(CAST(d.p_active AS varchar), '(null)'),
                                                    ' -> ',
                                                    COALESCE(CAST(d.active AS varchar), '(null)'))
                                    END
                               END
                    ) v
                    WHERE v.ColName IS NOT NULL
                )
            END IS NOT NULL
    ORDER BY d.VersionNum DESC;
    END;
    """)


def downgrade():
    op.execute("DROP PROCEDURE IF EXISTS [dbo].[GetReleaseItemHistoryById]")
