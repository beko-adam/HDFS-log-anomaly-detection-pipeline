import pandas as pd
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)

truth = pd.read_csv("anomaly_label.csv")
predicted = pd.read_csv("predictions.csv")

# Adjust these names to match your actual files.
truth = truth[["BlockId", "Label"]]
predicted = predicted[["BlockId", "Prediction"]]

assert not truth["BlockId"].duplicated().any()
assert not predicted["BlockId"].duplicated().any()

evaluation = truth.merge(
    predicted,
    on="BlockId",
    how="outer",
    validate="one_to_one",
    indicator=True,
)

print(evaluation["_merge"].value_counts())

# Missing predictions or labels must be investigated.
assert evaluation["_merge"].eq("both").all()

mapping = {"Normal": 0, "Anomaly": 1}

y_true = evaluation["Label"].map(mapping)
y_pred = evaluation["Prediction"].map(mapping)

assert y_true.notna().all()
assert y_pred.notna().all()

print(f"Accuracy:  {accuracy_score(y_true, y_pred):.4f}")
print(f"Precision: {precision_score(y_true, y_pred, zero_division=0):.4f}")
print(f"Recall:    {recall_score(y_true, y_pred, zero_division=0):.4f}")
print(f"F1:        {f1_score(y_true, y_pred, zero_division=0):.4f}")
print("Confusion matrix:")
print(confusion_matrix(y_true, y_pred, labels=[0, 1]))