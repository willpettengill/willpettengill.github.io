import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import Imputer


from sklearn.model_selection import cross_val_score
from sklearn.ensemble import RandomForestRegressor

def get_mae(X, y):
    # multiple by -1 to make positive MAE score instead of neg value returned as sklearn convention
    return -1 * cross_val_score(RandomForestRegressor(50), 
                                X, y, 
                                scoring = 'neg_mean_absolute_error').mean()

users = pd.io.parsers.read_csv('/Users/wpettengill/Desktop/willpettengill.github.io/ai_astrology/users.csv', dtype={'birthplacezipcode':'str'}).dropna().drop_duplicates(subset=['emailaddress'],keep='last')
users['birthplacezipcode'] = users['birthplacezipcode'].astype(str).str.zfill(5)

survey = pd.read_csv('/Users/wpettengill/Desktop/willpettengill.github.io/ai_astrology/survey.csv').dropna().drop_duplicates(subset=['emailaddress'],keep='last')

df=pd.merge(users, survey, how='inner')

train_predictors = df
categoricals = [df[col].name for col in df.columns if type(df[col][0]) is str]
one_hot_encoded_training_predictors = pd.get_dummies(train_predictors)
target = one_hot_encoded_training_predictors['ifeelpridefortheunitedstatesofamerica']
predictors_without_categoricals = train_predictors.drop(categoricals, axis=1)

mae_without_categoricals = get_mae(predictors_without_categoricals, target)

mae_one_hot_encoded = get_mae(one_hot_encoded_training_predictors, target)

print('Mean Absolute Error when Dropping Categoricals: ' + str(float(mae_without_categoricals)))
print('Mean Abslute Error with One-Hot Encoding: ' + str(float(mae_one_hot_encoded)))


#one_hot_encoded_test_predictors = pd.get_dummies(test_predictors)
#final_train, final_test = one_hot_encoded_training_predictors.align(one_hot_encoded_test_predictors, join='left', axis=1)

# make copy to avoid changing original data (when Imputing)
new_data = one_hot_encoded_training_predictors

# make new columns indicating what will be imputed
cols_with_missing = (col for col in new_data.columns 
                                 if new_data[col].isnull().any())
for col in cols_with_missing:
    new_data[col + '_was_missing'] = new_data[col].isnull()

# Imputation
my_imputer = Imputer()
new_data = my_imputer.fit_transform(new_data)






y = df.godisdeadandwehavekilledhim
X = data.select_dtypes(exclude=['object'])
train_X, test_X, train_y, test_y = train_test_split(X.as_matrix(), y.as_matrix(), test_size=0.25)

my_imputer = Imputer()
train_X = my_imputer.fit_transform(train_X)
test_X = my_imputer.transform(test_X)
