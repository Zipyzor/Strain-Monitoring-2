import pandas as pd
import os

# ==== File paths ====
microstrain_path = "converted_min_tube_strain_margin_to_max_microstrain_at_dll.csv"
controls_path = "NCR-Control-Elements.csv"
report_path = "ncr_max_control_vs_noncontrol_differences_with_maxes.csv"

# ==== Load CSVs ====
df_microstrain = pd.read_csv(microstrain_path)
df_controls = pd.read_csv(controls_path)

# ==== Clean and prepare ====
# Standardize element ID as string
df_microstrain.rename(columns={df_microstrain.columns[0]: "Element ID"}, inplace=True)
df_microstrain["Element ID"] = df_microstrain["Element ID"].astype(str)
df_controls["Element ID"] = df_controls["Element ID"].astype(str)

# Prepare output rows
report = []

# Group controls by NCR
for ncr, group in df_controls.groupby("NCR"):
    controls = group[group["Control"] == True]["Element ID"].tolist()
    non_controls = group[group["Control"] == False]["Element ID"].tolist()

    if not controls or not non_controls:
        continue

    # Get corresponding rows from microstrain file
    df_ctrl = df_microstrain[df_microstrain["Element ID"].isin(controls)].set_index("Element ID")
    df_nonctrl = df_microstrain[df_microstrain["Element ID"].isin(non_controls)].set_index("Element ID")

    if df_ctrl.empty or df_nonctrl.empty:
        continue

    # Transpose for subcase-wise comparison
    df_ctrl_t = df_ctrl.T
    df_nonctrl_t = df_nonctrl.T

    max_diff = 0
    max_subcase = None
    max_pair = (None, None)
    max_vals = (None, None)

    for subcase, ctrl_row in df_ctrl_t.iterrows():
        if subcase not in df_nonctrl_t.index:
            continue
        nonctrl_row = df_nonctrl_t.loc[subcase]

        for ctrl_elem, ctrl_val in ctrl_row.dropna().items():
            for nonctrl_elem, nonctrl_val in nonctrl_row.dropna().items():
                diff = abs(ctrl_val - nonctrl_val)
                if diff > max_diff:
                    max_diff = diff
                    max_subcase = subcase
                    max_pair = (ctrl_elem, nonctrl_elem)
                    max_vals = (ctrl_val, nonctrl_val)

    # Get max strain and corresponding subcase for both elements across all subcases
    if max_subcase:
        ctrl_all_subcases = df_ctrl.loc[max_pair[0]].dropna()
        nonctrl_all_subcases = df_nonctrl.loc[max_pair[1]].dropna()

        ctrl_max_strain = ctrl_all_subcases.max()
        ctrl_max_subcase = ctrl_all_subcases.idxmax()

        nonctrl_max_strain = nonctrl_all_subcases.max()
        nonctrl_max_subcase = nonctrl_all_subcases.idxmax()

        report.append({
            "NCR": ncr,
            "Control Element": max_pair[0],
            "Non-Control Element": max_pair[1],
            "Subcase (Max Difference)": max_subcase,
            "Control Strain (microstrain)": round(max_vals[0], 3),
            "Non-Control Strain (microstrain)": round(max_vals[1], 3),
            "Max Absolute Difference (microstrain)": round(max_diff, 3),
            "Control Max Strain (microstrain)": round(ctrl_max_strain, 3),
            "Control Max Subcase": ctrl_max_subcase,
            "Non-Control Max Strain (microstrain)": round(nonctrl_max_strain, 3),
            "Non-Control Max Subcase": nonctrl_max_subcase
        })

# ==== Create DataFrame and export ====
df_report = pd.DataFrame(report)
df_report.to_csv(report_path, index=False)

print(f"✅ Report saved as {report_path}")
