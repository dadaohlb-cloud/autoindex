import os
import joblib
import pandas as pd
import torch

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_absolute_error, mean_squared_error

from model.mlp import BenefitMLP


DROP_COLUMNS = [
    "sql_id",
    "candidate_id",
    "table_name",
    "sql_text",
    "index_type",
    "index_cols",
    "baseline_time",
    "indexed_time",
    "label",
]


def load_dataset(csv_path: str):
    df = pd.read_csv(csv_path)
    return df

def prepare_xy(df: pd.DataFrame):
    feature_cols = [c for c in df.columns if c not in DROP_COLUMNS]

    X = df[feature_cols].copy()
    y = df["label"].copy()

    # 全部转成数值；无法转换的变成 NaN
    for col in X.columns:
        X[col] = pd.to_numeric(X[col], errors="coerce")

    # 缺失值统一补 0
    X = X.fillna(0.0)

    # label 也保证是数值
    y = pd.to_numeric(y, errors="coerce").fillna(0.0)

    return X, y, feature_cols

def train_model(
    csv_path: str = "output/train.csv",
    model_path: str = "output/benefit_mlp.pt",
    scaler_path: str = "output/scaler.pkl",
    epochs: int = 200,
    lr: float = 1e-3,
    test_size: float = 0.2,
    random_state: int = 42,
):
    os.makedirs("output", exist_ok=True)

    df = load_dataset(csv_path)

    if len(df) < 5:
        raise ValueError("训练样本太少，至少先生成 5 条以上样本。")

    X, y, feature_cols = prepare_xy(df)
    print("\n=== Feature NaN Check ===")
    nan_count = X.isna().sum().sum()
    print(f"X total NaN count: {nan_count}")
    print(f"Feature columns: {len(feature_cols)}")

    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)

    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train_tensor = torch.tensor(y_train.values, dtype=torch.float32).view(-1, 1)

    X_val_tensor = torch.tensor(X_val_scaled, dtype=torch.float32)
    y_val_tensor = torch.tensor(y_val.values, dtype=torch.float32).view(-1, 1)

    model = BenefitMLP(input_dim=X_train_tensor.shape[1])
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = torch.nn.MSELoss()

    best_val_loss = float("inf")
    best_state = None

    for epoch in range(1, epochs + 1):
        model.train()
        pred = model(X_train_tensor)
        loss = loss_fn(pred, y_train_tensor)

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        model.eval()
        with torch.no_grad():
            val_pred = model(X_val_tensor)
            val_loss = loss_fn(val_pred, y_val_tensor)

        if val_loss.item() < best_val_loss:
            best_val_loss = val_loss.item()
            best_state = model.state_dict()

        if epoch % 20 == 0 or epoch == 1:
            print(
                f"Epoch {epoch:03d} | "
                f"train_loss={loss.item():.6f} | "
                f"val_loss={val_loss.item():.6f}"
            )

    if best_state is not None:
        model.load_state_dict(best_state)

    model.eval()
    with torch.no_grad():
        train_pred = model(X_train_tensor).cpu().numpy().reshape(-1)
        val_pred = model(X_val_tensor).cpu().numpy().reshape(-1)

    train_mae = mean_absolute_error(y_train, train_pred)
    val_mae = mean_absolute_error(y_val, val_pred)

    train_mse = mean_squared_error(y_train, train_pred)
    val_mse = mean_squared_error(y_val, val_pred)

    torch.save(
        {
            "model_state_dict": model.state_dict(),
            "input_dim": X_train_tensor.shape[1],
            "feature_cols": feature_cols,
        },
        model_path,
    )

    joblib.dump(scaler, scaler_path)

    print("\n=== 训练完成 ===")
    print(f"训练集 MAE: {train_mae:.6f}")
    print(f"验证集 MAE: {val_mae:.6f}")
    print(f"训练集 MSE: {train_mse:.6f}")
    print(f"验证集 MSE: {val_mse:.6f}")
    print(f"模型已保存到: {model_path}")
    print(f"Scaler 已保存到: {scaler_path}")

    return model, scaler, feature_cols


if __name__ == "__main__":
    train_model()