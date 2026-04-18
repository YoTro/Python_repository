# Deprecated: use src.chat_bot instead.
# Kept for backward compatibility only.
from src.chat_bot import run_chat_sessions as _run


def run_chat_sessions(output_path, **kwargs):
    return _run(platform='zhipin', output_path=output_path, **kwargs)
