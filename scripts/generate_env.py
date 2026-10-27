import base64
import secrets
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = ROOT / ".env.example"
OUTPUT_PATH = ROOT / ".env"
SECRET_NAMES = {
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_WEBSERVER_SECRET_KEY",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_META_DB_PASSWORD",
    "WAREHOUSE_DB_PASSWORD",
}


def generate_environment(output_path: Path = OUTPUT_PATH) -> None:
    template_lines = TEMPLATE_PATH.read_text(encoding="utf-8").splitlines()
    secrets_by_name = {
        "AIRFLOW_FERNET_KEY": base64.urlsafe_b64encode(
            secrets.token_bytes(32)
        ).decode("ascii"),
        "AIRFLOW_WEBSERVER_SECRET_KEY": secrets.token_urlsafe(48),
        "AIRFLOW_ADMIN_PASSWORD": secrets.token_urlsafe(24),
        "AIRFLOW_META_DB_PASSWORD": secrets.token_urlsafe(24),
        "WAREHOUSE_DB_PASSWORD": secrets.token_urlsafe(24),
    }

    found_names = set()
    for index, line in enumerate(template_lines):
        name, separator, _ = line.partition("=")
        if separator and name in SECRET_NAMES:
            if name in found_names:
                raise ValueError(
                    f"Variable duplicada en .env.example: {name}"
                )
            template_lines[index] = f"{name}={secrets_by_name[name]}"
            found_names.add(name)

    if found_names != SECRET_NAMES:
        missing_names = ", ".join(sorted(SECRET_NAMES - found_names))
        raise ValueError(
            f"Faltan variables secretas en .env.example: {missing_names}"
        )

    with output_path.open("x", encoding="utf-8", newline="\n") as env_file:
        env_file.write("\n".join(template_lines) + "\n")


if __name__ == "__main__":
    try:
        generate_environment()
    except FileExistsError:
        raise SystemExit(".env ya existe; no se sobrescribió.") from None
    print(f"Archivo .env creado en {OUTPUT_PATH}")
