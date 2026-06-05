from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "data" / "processed"
TARGET = ROOT / "dashboard" / "public" / "data"


def main() -> None:
    TARGET.mkdir(parents=True, exist_ok=True)
    for source in SOURCE.glob("*.parquet"):
        df = pd.read_parquet(source).copy()
        for column in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[column]):
                df[column] = df[column].dt.strftime("%Y-%m-%d")
        df.to_json(TARGET / f"{source.stem}.json", orient="records", force_ascii=False)
        print(f"Exportado: {source.name}")


if __name__ == "__main__":
    main()
