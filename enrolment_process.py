import pandas as pd
import glob
import re
from rapidfuzz import process, fuzz

# -------------------------------
# LOAD DISTRICT MASTER
# -------------------------------
district_master = pd.read_csv("master_data/district_master.csv")

district_master["state"] = (
    district_master["state"]
    .astype(str)
    .str.replace("&", "And", regex=False)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.title()
)

district_master["district"] = (
    district_master["district"]
    .astype(str)
    .str.replace(r"\s+", " ", regex=True)
    .str.strip()
    .str.title()
)

state_district_map = (
    district_master.groupby("state")["district"]
    .apply(list)
    .to_dict()
)

INVALID_VALUES = {"0", "100000", "?", "nan", "none", ""}

# -------------------------------
# CLEANING FUNCTIONS
# -------------------------------
def normalize_state(state):
    if pd.isna(state):
        return None

    state = str(state).strip().lower()

    if state in INVALID_VALUES or re.search(r"\d", state):
        return None

    return state.replace("&", "and").title()

def correct_district_name(state, district, threshold=90):
    if not state or pd.isna(district):
        return None

    district = str(district).strip().lower()

    if district in INVALID_VALUES or re.search(r"\d", district):
        return None

    district = district.title()
    candidates = state_district_map.get(state)

    if not candidates:
        return None

    match = process.extractOne(
        district, candidates, scorer=fuzz.token_sort_ratio
    )

    return match[0] if match and match[1] >= threshold else None

# -------------------------------
# PROCESS ENROLMENT FILES
# -------------------------------
files = glob.glob("data/enrolment/*.csv")
all_chunks = []

for file in files:
    print(f"Processing {file}...")

    for chunk in pd.read_csv(file, chunksize=200_000):

        chunk["state"] = chunk["state"].apply(normalize_state)
        chunk["district"] = chunk.apply(
            lambda r: correct_district_name(r["state"], r["district"]),
            axis=1
        )

        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")

        # HARD FILTER
        chunk = chunk.dropna(subset=["date", "state", "district"])

        agg = (
            chunk
            .groupby(["date", "state", "district"], as_index=False)
            [["age_0_5", "age_5_17", "age_18_greater"]]
            .sum()
        )

        all_chunks.append(agg)

enrol_district = pd.concat(all_chunks, ignore_index=True)

enrol_district = (
    enrol_district
    .groupby(["date", "state", "district"], as_index=False)
    .sum()
)

enrol_district["total_enrolment"] = (
    enrol_district["age_0_5"]
    + enrol_district["age_5_17"]
    + enrol_district["age_18_greater"]
)

enrol_district.to_csv("output/enrolment_district.csv", index=False)
print("✅ Enrolment processing completed successfully!")
