import pandas as pd
import numpy as np
import re
import os
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

def analyze_rank_sales():
    file_path = "data/Patio-Lawn-Garden-sale-rank-amazon.csv"
    try:
        df = pd.read_csv(file_path, encoding='gbk')
    except:
        df = pd.read_csv(file_path, encoding='latin1')

    print(f"DEBUG: First 3 rows:\n{df.head(3)}")

    # 1. Clean Rank data: extract the number after any non-digit separator
    def extract_rank(rank_str):
        if pd.isna(rank_str): return None
        # Try to find any sequence of digits after a separator like ':' or '锛'
        # Or just find the last number in the string which is usually the rank
        nums = re.findall(r'(\d[\d,]*)', str(rank_str))
        if nums:
            # Clean commas and return the last or largest number (rank is usually large)
            return int(nums[-1].replace(',', ''))
        return None

    df['Rank_Num'] = df['PrimaryRank'].apply(extract_rank)
    df['Orders'] = pd.to_numeric(df['Orders'], errors='coerce')

    # 2. Filter valid data (Orders > 0 and Rank > 1)
    # Formula uses ln(Rank - 1), so Rank must be > 1. ln(Q) requires Q > 0.
    mask = (df['Orders'] > 0) & (df['Rank_Num'] > 1)
    df_clean = df[mask].copy()

    if df_clean.empty:
        print("No valid data for regression (need Orders > 0 and Rank > 1).")
        return

    # 3. Prepare variables for linear regression
    # Rearranged formula: ln(Q) = (1/theta) * ln(Rank - 1) - (c/theta)
    # Let y = ln(Q), x = ln(Rank - 1)
    # y = m*x + b  => m = 1/theta, b = -c/theta
    y = np.log(df_clean['Orders']).values.reshape(-1, 1)
    x = np.log(df_clean['Rank_Num'] - 1).values.reshape(-1, 1)

    model = LinearRegression()
    model.fit(x, y)

    m = model.coef_[0][0]
    b = model.intercept_[0]
    r_squared = model.score(x, y)

    # 4. Derive theta and c
    # theta = 1/m
    # b = -c/theta => c = -b * theta = -b / m
    theta = 1.0 / m
    c = -b / m

    print(f"--- Analysis Results ---")
    print(f"Samples count: {len(df_clean)}")
    print(f"Coefficient (1/theta): {m:.4f}")
    print(f"Intercept (-c/theta): {b:.4f}")
    print(f"Estimated theta: {theta:.4f}")
    print(f"Estimated c: {c:.4f}")
    print(f"R-squared (Fit accuracy): {r_squared:.4f}")

    # 5. Plotting
    plt.figure(figsize=(10, 6))
    plt.scatter(x, y, alpha=0.5, label='Actual Data (Log-Log)')
    plt.plot(x, model.predict(x), color='red', label='UCLA Formula Fit')
    plt.xlabel('ln(Rank - 1)')
    plt.ylabel('ln(Orders)')
    plt.title(f'Amazon Sales vs Rank Analysis (R² = {r_squared:.4f})\nCategory: Patio, Lawn & Garden')
    plt.legend()
    plt.grid(True)
    
    # Save the plot to data folder
    os.makedirs("data", exist_ok=True)
    plot_path = "data/sales_rank_fit.png"
    plt.savefig(plot_path)
    print(f"Visualization saved to {plot_path}")

    if r_squared > 0.7:
        print("Conclusion: The data shows a strong fit with the UCLA power-law formula.")
    elif r_squared > 0.4:
        print("Conclusion: The data shows a moderate fit. Real-world factors (promotions, seasonality) may cause deviations.")
    else:
        print("Conclusion: The data does not strongly follow the formula for this specific period/category.")

if __name__ == "__main__":
    analyze_rank_sales()
