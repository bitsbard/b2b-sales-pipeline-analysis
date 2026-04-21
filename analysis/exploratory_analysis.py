"""
MavenTech B2B Sales Pipeline — Exploratory Analysis
====================================================
Generates the five charts in /charts/ and prints key metrics
used in the README and the VP insights memo.

Usage:
    cd B2B_Sales_Pipeline_Analysis
    pip install pandas matplotlib seaborn
    python analysis/exploratory_analysis.py
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
DATA = BASE / "CRM+Sales+Opportunities"
OUT  = BASE / "charts"
OUT.mkdir(exist_ok=True)

pipeline = pd.read_csv(DATA / "sales_pipeline.csv")
products = pd.read_csv(DATA / "products.csv")
teams    = pd.read_csv(DATA / "sales_teams.csv")

pipeline['engage_date']    = pd.to_datetime(pipeline['engage_date'])
pipeline['close_date']     = pd.to_datetime(pipeline['close_date'])
pipeline['reached_engaging'] = pipeline['engage_date'].notna()
pipeline['is_closed']      = pipeline['deal_stage'].isin(['Won', 'Lost'])
pipeline['is_won']         = pipeline['deal_stage'] == 'Won'
pipeline['days_to_close']  = (
    (pipeline['close_date'] - pipeline['engage_date']).dt.days
    .where(pipeline['is_closed'] & pipeline['reached_engaging'])
)
pipeline['quarter_opened'] = pipeline['engage_date'].dt.to_period('Q').astype(str)
pipeline['month_opened']   = pipeline['engage_date'].dt.to_period('M').astype(str)
pipeline['quarter_closed'] = pipeline['close_date'].dt.to_period('Q').astype(str)

pl = (pipeline
      .merge(products, on='product', how='left')
      .merge(teams,    on='sales_agent', how='left'))

BRAND  = "#1B5E9B"
ACCENT = "#E85D4A"
GREEN  = "#2D9E5F"
GRAY   = "#555555"

plt.rcParams.update({
    'font.family': 'DejaVu Sans',
    'axes.spines.top': False, 'axes.spines.right': False,
    'axes.grid': True, 'grid.alpha': 0.3, 'grid.linestyle': '--',
})

# ── metrics ──────────────────────────────────────────────────
total   = len(pipeline)
engaged = int(pipeline['reached_engaging'].sum())
closed  = int(pipeline['is_closed'].sum())
won     = int(pipeline['is_won'].sum())
lost    = int((pipeline['deal_stage'] == 'Lost').sum())
won_df  = pipeline[pipeline['is_won']]

print(f"Total:            {total:,}")
print(f"Reached Engaging: {engaged:,}  ({engaged/total*100:.1f}%)")
print(f"Won:              {won:,}  ({won/closed*100:.1f}% of closed)")
print(f"Lost:             {lost:,}  ({lost/closed*100:.1f}% of closed)")
print(f"Revenue:          ${won_df['close_value'].sum():,.0f}")
print(f"Avg Deal Size:    ${won_df['close_value'].mean():,.0f}")
print(f"Avg Cycle (days): {won_df['days_to_close'].mean():.1f}")

prod_df = pl[pl['is_closed']].groupby('product').agg(
    total=('is_closed','sum'), won=('is_won','sum'),
    avg_acv=('close_value','mean'), avg_cycle=('days_to_close','mean')
).reset_index()
prod_df['win_rate'] = prod_df['won'] / prod_df['total']
prod_df = prod_df.sort_values('win_rate')

rep_df = pl[pl['is_closed']].groupby(['sales_agent','manager','regional_office']).agg(
    total=('is_closed','sum'), won=('is_won','sum'),
    revenue=('close_value','sum'), avg_cycle=('days_to_close','mean')
).reset_index()
rep_df['win_rate'] = rep_df['won'] / rep_df['total']
rep_df = rep_df.sort_values('win_rate')

# ── Chart 1: Funnel ──────────────────────────────────────────
fig, ax = plt.subplots(figsize=(9,5))
counts = [total, engaged, won]
colors = [BRAND, "#3A85C7", GREEN]
bars = ax.barh(['Prospecting\n(All Opps)','Reached\nEngaging','Closed Won'][::-1],
               counts[::-1], color=colors[::-1], height=0.5, edgecolor='white')
for bar, count in zip(bars, counts[::-1]):
    ax.text(bar.get_width()+80, bar.get_y()+bar.get_height()/2,
            f'{count:,}  ({count/total*100:.1f}%)', va='center', fontsize=12, color=GRAY, fontweight='bold')
ax.set_xlim(0, 11200)
ax.set_xlabel('Number of Opportunities', fontsize=11)
ax.set_title('MavenTech Sales Funnel — Stage-by-Stage Conversion', fontsize=14, fontweight='bold', pad=15)
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x,_: f'{int(x):,}'))
ax.grid(axis='y', alpha=0)
ax.annotate(f'{engaged/total*100:.1f}% reach Engaging', xy=(8300,1.5), xytext=(5800,1.75),
            fontsize=10, color=ACCENT, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color=ACCENT, lw=1.5))
ax.annotate(f'{won/closed*100:.1f}% of closed\ndeals are Won', xy=(4238,0.5), xytext=(1800,0.2),
            fontsize=10, color=ACCENT, fontweight='bold',
            arrowprops=dict(arrowstyle='->', color=ACCENT, lw=1.5))
plt.tight_layout()
plt.savefig(OUT/"01_funnel.png", dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 2: Revenue Trend ────────────────────────────────────
q_stats = (pipeline[pipeline['is_won']].groupby('quarter_closed')
           .agg(deals=('opportunity_id','count'), revenue=('close_value','sum'))
           .reset_index().sort_values('quarter_closed'))
fig, ax1 = plt.subplots(figsize=(9,5))
x = range(len(q_stats))
bars = ax1.bar(x, q_stats['revenue'], color=BRAND, alpha=0.8)
ax1.set_xticks(x); ax1.set_xticklabels(q_stats['quarter_closed'], fontsize=11)
ax1.set_ylabel('Closed-Won Revenue ($)', fontsize=11, color=BRAND)
ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v,_: f'${v/1e6:.1f}M'))
ax1.tick_params(axis='y', colors=BRAND)
ax2 = ax1.twinx()
ax2.plot(x, q_stats['deals'], color=ACCENT, marker='o', linewidth=2.5, markersize=8)
ax2.set_ylabel('Deals Closed-Won', fontsize=11, color=ACCENT)
ax2.tick_params(axis='y', colors=ACCENT); ax2.spines['top'].set_visible(False)
for bar, rev in zip(bars, q_stats['revenue']):
    ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+45000,
             f'${rev/1e6:.2f}M', ha='center', fontsize=10, fontweight='bold', color=BRAND)
ax1.set_title('Revenue Trend — Closed-Won by Quarter', fontsize=14, fontweight='bold', pad=15)
ax1.spines['top'].set_visible(False)
p1 = mpatches.Patch(color=BRAND, alpha=0.8, label='Revenue ($)')
p2 = plt.Line2D([0],[0], color=ACCENT, marker='o', linewidth=2, label='Deals Won')
ax1.legend(handles=[p1,p2], loc='upper left', fontsize=10)
plt.tight_layout()
plt.savefig(OUT/"02_revenue_trend.png", dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 3: Win Rate by Product ──────────────────────────────
avg_wr = prod_df['win_rate'].mean()
bar_colors = [ACCENT if wr < avg_wr else BRAND for wr in prod_df['win_rate']]
fig, ax = plt.subplots(figsize=(9,5))
bars = ax.barh(prod_df['product'], prod_df['win_rate']*100, color=bar_colors, height=0.55, edgecolor='white')
ax.axvline(avg_wr*100, color=GRAY, linestyle='--', linewidth=1.5)
for bar, wr, n in zip(bars, prod_df['win_rate'], prod_df['total']):
    ax.text(bar.get_width()+0.3, bar.get_y()+bar.get_height()/2,
            f'{wr*100:.1f}%  (n={n})', va='center', fontsize=10, color=GRAY)
ax.set_xlim(0, 82); ax.set_xlabel('Win Rate (%)', fontsize=11)
ax.set_title('Win Rate by Product (closed deals only)', fontsize=14, fontweight='bold', pad=15)
ax.legend(handles=[mpatches.Patch(color=BRAND,label='At/above avg'),
                   mpatches.Patch(color=ACCENT,label='Below avg'),
                   plt.Line2D([0],[0],color=GRAY,linestyle='--',label=f'Avg {avg_wr*100:.1f}%')],
          fontsize=10, loc='lower right')
ax.grid(axis='y', alpha=0)
plt.tight_layout()
plt.savefig(OUT/"03_win_rate_product.png", dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 4: Rep Win Rate ─────────────────────────────────────
rep_sorted = rep_df.sort_values('win_rate', ascending=True)
rc = {'Central': BRAND, 'East': GREEN, 'West': ACCENT}
bar_colors = [rc.get(r, GRAY) for r in rep_sorted['regional_office']]
fig, ax = plt.subplots(figsize=(10,8))
ax.barh(rep_sorted['sales_agent'], rep_sorted['win_rate']*100, color=bar_colors, height=0.7, edgecolor='white')
avg_rep = rep_sorted['win_rate'].mean()*100
ax.axvline(avg_rep, color=GRAY, linestyle='--', linewidth=1.5)
for i, (_, row) in enumerate(rep_sorted.iterrows()):
    ax.text(row['win_rate']*100+0.2, i, f"{row['win_rate']*100:.1f}%", va='center', fontsize=9, color=GRAY)
ax.set_xlim(0,83); ax.set_xlabel('Win Rate (%)', fontsize=11)
ax.set_title('Sales Rep Win Rate — All Reps\n(closed deals, colored by region)', fontsize=14, fontweight='bold', pad=15)
handles = [mpatches.Patch(color=v,label=k) for k,v in rc.items()]
handles.append(plt.Line2D([0],[0], color=GRAY, linestyle='--', label=f'Avg {avg_rep:.1f}%'))
ax.legend(handles=handles, fontsize=10, loc='lower right')
ax.grid(axis='y', alpha=0)
plt.tight_layout()
plt.savefig(OUT/"04_rep_win_rate.png", dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 5: Cohort Heatmap ───────────────────────────────────
cohort_df = pl[pl['is_closed']].copy()
cohort_pivot = (cohort_df.groupby(['month_opened','quarter_closed'])
                .apply(lambda g: g['is_won'].sum()/len(g), include_groups=False)
                .unstack(fill_value=np.nan))
cohort_pivot = cohort_pivot.dropna(how='all').iloc[:,:4]
fig, ax = plt.subplots(figsize=(10,8))
sns.heatmap(cohort_pivot*100, annot=True, fmt='.0f', cmap='YlGn',
            mask=cohort_pivot.isna(), ax=ax,
            linewidths=0.5, linecolor='white',
            cbar_kws={'label':'Win Rate (%)'},
            vmin=40, vmax=85)
ax.set_title('Deal Cohort Heatmap — Win Rate by Engage Month × Close Quarter',
             fontsize=14, fontweight='bold', pad=15)
ax.set_xlabel('Quarter Closed', fontsize=11); ax.set_ylabel('Month Engaged', fontsize=11)
plt.xticks(rotation=0); plt.yticks(rotation=0)
plt.tight_layout()
plt.savefig(OUT/"05_cohort_heatmap.png", dpi=150, bbox_inches='tight'); plt.close()

print("Done. Charts written to:", OUT)
