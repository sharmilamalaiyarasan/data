import pandas as pd

enrol = pd.read_csv("output/enrolment_district.csv")
demo  = pd.read_csv("output/demographic_district.csv")
bio   = pd.read_csv("output/biometric_district.csv")

for df in [enrol, demo, bio]:
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

keys = pd.concat([
    enrol[["date", "state", "district"]],
    demo[["date", "state", "district"]],
    bio[["date", "state", "district"]],
]).drop_duplicates()

master = (
    keys
    .merge(enrol, on=["date","state","district"], how="left")
    .merge(demo,  on=["date","state","district"], how="left")
    .merge(bio,   on=["date","state","district"], how="left")
)

num_cols = master.select_dtypes(include="number").columns
master[num_cols] = master[num_cols].fillna(0)

master["enrolment_available"] = (master["total_enrolment"] > 0).astype(int)
master["demo_data_available"] = (master["total_demographic_updates"] > 0).astype(int)
master["bio_data_available"]  = (master["total_biometric_updates"] > 0).astype(int)

master = master.sort_values(["date","state","district"])
master.to_csv("output/uidai_master_clean.csv", index=False)

print("✅ UIDAI Master dataset created successfully!")
print("📊 Rows:", master.shape[0], "| Columns:", master.shape[1])
