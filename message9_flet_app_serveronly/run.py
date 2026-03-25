import flet as ft

if __package__ in (None, ""):
    from pathlib import Path
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from message9_flet_app_serveronly.client import main
else:
    from .client import main


if __name__ == "__main__":
    ft.app(target=main)
