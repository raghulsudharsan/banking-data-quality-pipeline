import pandas as pd

application = pd.read_csv(
    "data/raw/application_train.csv",
    nrows=1000
)

bureau = pd.read_csv(
    "data/raw/bureau.csv",
    nrows=1000
)

print("\nAPPLICATION TRAIN")
print("=" * 60)
print("Shape:", application.shape)

print("\nColumns:")
print(application.columns.tolist())

print("\nData types:")
print(application.dtypes)

print("\nMissing values:")
print(application.isna().sum().sort_values(ascending=False).head(10))

print("\nBUREAU")
print("=" * 60)
print("Shape:", bureau.shape)

print("\nColumns:")
print(bureau.columns.tolist())

print("\nData types:")
print(bureau.dtypes)

print("\nMissing values:")
print(bureau.isna().sum().sort_values(ascending=False).head(10))
