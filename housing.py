#############################################################################
######################## Qualité logements New York #########################
#############################################################################

from pprint import pprint #pour afficher les résultats des curseurs mongodb
import pandas as pd
import pymongo

#####################################
######### IMPORT DE LA BASE #########
#####################################

housing  = pd.read_csv('./housing.csv')
housing
### Codes des états dans airhealth ###
# 1 - Bronx
# 2 - Brooklyn
# 3 - Manhattan
# 4 - Queens
# 5 - Staten Island

### Ici c'est pas exactement pareil donc on va les mettre comme avant
# Anciens codes boroughs
orep = housing['BoroID'].unique()
#ID Boroughs réécrits en accord avec airhealth
nrep = [4,1,3,2,5]
#On applique la nouvelle mise en forme des boroughs ID
housing['BoroID'].replace(orep,nrep,inplace=True)
housing.dtypes
housing[housing.isnull().any(axis=1)]
del housing['CaseOpenDate']
housing.shape
housing.fillna(value = '-1',inplace = True)

housing['CaseType'].unique()

########################################
######## UTILISATION DE MONGODB ########
########################################

client = pymongo.MongoClient()
# client.drop_database('test')
db = client.test
# db.houseMG.drop()
houseMG = db.houseMG
houseMG.insert_many(housing.to_dict('records'))
pprint(list(houseMG.find()))
pipeline =[
    {
        "$group": {
            "_id": "$Boro",
            "nbrCases": { "$sum": 1 },
            "yesCases": {
                "$sum": {
                    "$cond": [ { "$eq": [ "$CaseJudgement", "YES" ] }, 1, 0 ]
                }
            },
            "noCases": {
                "$sum": {
                    "$cond": [ { "$eq": [ "$CaseJudgement", "NO" ] }, 1, 0 ]
                }
            }
        }
    },
    {"$project" : {"_id":1,"nbrCases" : 1,"yesCases":1,"noCases":1,
    "ratioY":{ "$divide": [ "$yesCases", "$nbrCases" ] },
    "ratioN":{ "$divide": [ "$noCases", "$nbrCases" ] }}},
    {"$sort":  {"ratioY" : -1} }]
pprint(list(houseMG.aggregate(pipeline)))
