import pandas as pd
import matplotlib.pyplot as plt
import os

# CONFIG
CSV_FILE = 'nutrition_activity_obesity_usa_subset.csv'
IMAGES_DIR = 'images'

# Ensure images directory exists
os.makedirs(IMAGES_DIR, exist_ok=True)

# Load dataset
df = pd.read_csv(CSV_FILE)


question_1 = 'Percent of adults who engage in no leisure-time physical activity'
state_1 = 'California'

df_trend = df[(df['Question'] == question_1) & (df['LocationDesc'] == state_1)]
yearly_mean = df_trend.groupby('YearStart')['Data_Value'].mean()

plt.figure(figsize=(8, 5))
yearly_mean.plot(marker='o', linestyle='-', color='blue')
plt.title(f'Trend over Time ({state_1}) - {question_1}')
plt.xlabel('Year')
plt.ylabel('Mean Data_Value (%)')
plt.grid(True)
plt.tight_layout()
plt.savefig(f'{IMAGES_DIR}/trend_over_time_{state_1.replace(" ", "_")}.png')
plt.close()

question_2 = 'Percent of adults aged 18 years and older who have obesity'

df_category = df[df['Question'] == question_2]
category_means = df_category.groupby('StratificationCategory1')['Data_Value'].mean().sort_values()

plt.figure(figsize=(8, 5))
category_means.plot(kind='barh', color='coral')
plt.title(f'Mean Data_Value by Category - {question_2}')
plt.xlabel('Mean Data_Value (%)')
plt.ylabel('Category')
plt.grid(axis='x', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig(f'{IMAGES_DIR}/mean_by_category_{question_2[:20].replace(" ", "_")}.png')
plt.close()


question_3 = 'Percent of adults aged 18 years and older who have obesity'
states_to_compare = ['California', 'Texas', 'New York', 'Florida', 'Illinois']

df_compare = df[(df['Question'] == question_3) & (df['LocationDesc'].isin(states_to_compare))]
state_year_mean = df_compare.groupby(['LocationDesc', 'YearStart'])['Data_Value'].mean().unstack()

state_year_mean.T.plot(kind='bar', figsize=(10, 6))
plt.title(f'Comparison of States over Years - {question_3}')
plt.xlabel('Year')
plt.ylabel('Mean Data_Value (%)')
plt.legend(title='State')
plt.grid(axis='y', linestyle='--', alpha=0.6)
plt.tight_layout()
plt.savefig(f'{IMAGES_DIR}/state_comparison_{question_3[:20].replace(" ", "_")}.png')
plt.close()



question_4 = 'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week'
state_4 = 'New York'

df_pie = df[(df['Question'] == question_4) & (df['LocationDesc'] == state_4)]
strat_counts = df_pie['Stratification1'].value_counts()

plt.figure(figsize=(7, 7))
strat_counts.plot(kind='pie', autopct='%1.1f%%', startangle=140)
plt.title(f'Stratification1 Distribution - {state_4} - {question_4}')
plt.ylabel('')
plt.tight_layout()
plt.savefig(f'{IMAGES_DIR}/stratification_pie_{state_4.replace(" ", "_")}.png')
plt.close()
