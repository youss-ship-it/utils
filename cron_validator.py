import re

# Standard 5-field cron ranges:
# ┌───────────── minute (0–59)
# │ ┌───────────── hour (0–23)
# │ │ ┌───────────── day of month (1–31)
# │ │ │ ┌───────────── month (1–12)
# │ │ │ │ ┌───────────── day of week (0–7, both 0 and 7 represent Sunday)
# │ │ │ │ │
# * * * * *

_FIELD_RANGES = [
    (0, 59),
    (0, 23),
    (1, 31),
    (1, 12),
    (0, 7),
]


def is_valid_cron(expression: str) -> bool:
    """
    Validate a standard 5-field cron expression.

    Supports: wildcard (*), values (5), lists (1,3,5),
    ranges (1-5), and steps (*/15, 1-5/2).

    Does not support named months/days (JAN, MON)
    or special strings (@daily, @weekly).
    """
    if not expression or not expression.strip():
        return False

    fields = expression.strip().split()
    if len(fields) != 5:
        return False

    return all(
        _is_valid_field(field, min_val, max_val)
        for field, (min_val, max_val) in zip(fields, _FIELD_RANGES)
    )


def _is_valid_field(field: str, min_val: int, max_val: int) -> bool:
    """A field is a comma-separated list of parts."""
    if not field:
        return False
    return all(
        _is_valid_part(part, min_val, max_val)
        for part in field.split(",")
    )


def _is_valid_part(part: str, min_val: int, max_val: int) -> bool:
    """A part is one of: *, N, N-N, or any of the above with /step."""
    if not part:
        return False

    if "/" in part:
        base, _, step = part.partition("/")
        if not step.isdigit() or int(step) == 0:
            return False
        if base == "*":
            return True
        return _is_valid_range_or_value(base, min_val, max_val)

    if part == "*":
        return True

    return _is_valid_range_or_value(part, min_val, max_val)


def _is_valid_range_or_value(part: str, min_val: int, max_val: int) -> bool:
    """Either a single integer or a low-high range."""
    if "-" in part:
        bounds = part.split("-", 1)
        if len(bounds) != 2 or not all(b.isdigit() for b in bounds):
            return False
        low, high = int(bounds[0]), int(bounds[1])
        return min_val <= low <= high <= max_val

    if part.isdigit():
        return min_val <= int(part) <= max_val

    return False


class VDBService(SessionManagerMixin):
    # ...

    @staticmethod
    def _validate_auto_refresh_schedule(schedule: str | None) -> None:
        """
        Validate that the provided cron expression is syntactically correct.
        None is valid — it means "disable auto-refresh".
        """
        if schedule is None:
            return
        if not is_valid_cron(schedule):
            raise VDBServiceError(
                f"Invalid cron expression '{schedule}'. "
                "Expected a standard 5-field cron format (e.g. '0 2 * * 0')."
            )

    def update(self, vdb_id: str, **kwargs) -> VDB:
        try:
            vdb = self.repositories.vdb.get_by_id(vdb_id)
        except NotFoundError as e:
            raise VDBServiceError(f"VDB with id '{vdb_id}' is not found") from e

        invalid_keys = [key for key in kwargs if not hasattr(vdb, key)]
        if invalid_keys:
            raise VDBServiceError(
                f"Invalid update fields: '{', '.join(invalid_keys)}'. "
                "Please provide valid VDB attributes."
            )

        # validate cron before persisting
        if "auto_refresh_schedule" in kwargs:
            self._validate_auto_refresh_schedule(kwargs["auto_refresh_schedule"])

        changes = {
            attr: value
            for attr, value in kwargs.items()
            if getattr(vdb, attr, None) != value
        }
        if not changes:
            return vdb

        try:
            with self.autocommit():
                self.repositories.vdb.update(vdb_id, **changes)
        except Exception as e:
            raise VDBServiceError(
                f"Error while updating VDB '{vdb_id}' with changes {changes}: {str(e)}"
            ) from e

        return self.repositories.vdb.get_by_id(vdb_id)
