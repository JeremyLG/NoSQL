#############################################################################
########################### Activités à New York ############################
#############################################################################

from pprint import pprint #pour afficher les résultats des curseurs mongodb
import pandas as pd
import pymongo

#####################################
######### IMPORT DE LA BASE #########
#####################################

activities  = pd.read_csv('./activities.csv')

rows = activities['the_geom'].size

#activities['the_geom'].apply(len).unique()

#On modifie le string des positions en deux colonnes latitude et longitude
for i in range(rows):

    string = activities.ix[i,0]
    j = 0

    while string[j] != '(':
        j = j+1
    activities.ix[i,'longitude'] = float(string[j+1:j+10])

    while string[j] != ' ':
        j = j+1
    activities.ix[i,'latitude'] = float(string[j+1:j+11])
activities.drop(activities.columns[[0,1,4,5,6,7,8]],axis=1,inplace = True)
activities.columns

activities.groupby('SITE_BOROU').count()
activities.ix[20,:]
activities['WEEKLY_HOU'].unique()

activities.shape
activities['SETTING'].unique()
activities['SETTING'].unique()
activities.replace(['yes','Yes','Yes\'','No','unknown'],[1,1,1,0,0],inplace = True)
activities.dtypes

########################################
######## UTILISATION DE MONGODB ########
########################################
client = pymongo.MongoClient()
# client.drop_database('test')
db = client.test
# db.activitiesMG.drop()
activitiesMG = db.activitiesMG
activitiesMG.insert_many(activities.to_dict('records'))
pprint(list(activitiesMG.find()))
pipeline =[
{'$match':{'SITE_BOROU':'Staten Island'}},
    {"$project":{
    '_id':0,
    'zip':'$SITE_ZIP',
    'borough':'$SITE_BOROU',
    'street':'$SITE_STREE',
    'sum' : {'$sum' : [ '$ACADEMICS', '$SPORTS_PHY','$SUMMER','$WEEKENDS','$ARTS_CULTU',
    '$ELEMENTARY','$EVENINGS','$HIGH_SCHOO','$MIDDLE_SCH','$SCHOOL_YEA', ]}}},
    {'$sort':{'sum':-1}}]
pprint(list(activitiesMG.aggregate(pipeline)))
