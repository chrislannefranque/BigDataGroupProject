import pandas as pd
import matplotlib.pyplot as plt
from sklearn.svm import SVC

train_data = pd.read_csv("Dataset/train.csv")

train_x = train_data.drop(columns="isFraud").to_numpy()

train_y = train_data["isFraud"].to_numpy()

clf = SVC(kernel='rbf')
clf.fit(train_x, train_y)

val_data = pd.read_csv("Dataset/validation.csv")

val_x = val_data.drop(columns="isFraud").to_numpy()

val_y = val_data["isFraud"].to_numpy()

print(clf.score(val_x, val_y))









