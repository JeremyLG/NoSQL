#############################################################################
######################## Qualité de l'air à New York ########################
#############################################################################

from pprint import pprint #pour afficher les résultats des curseurs mongodb
import pandas as pd
import pymongo

#####################################
######## CLEANING DE LA BASE ########
#####################################

airhealth = pd.read_csv('./airhealth.csv')
airhealth
airhealth[airhealth.isnull().any(axis=1)] ##Pas de valeurs manquantes !
airhealth['name'].unique() ## Les descriptions des toxicités calculées sur l'air dans les quartiers
airhealth ['year_description'].unique() ## On peut voir que la colonne year est mal remplie
## On va même kick les annual average : 'Annual Average 2009-2010','2-Year Winter Average 2009-2010' et ne garder que :
years = ['2005', '2013','2005-2007', '2009-2011']
airhealth = airhealth[~airhealth.year_description.isin(years)==False]

airhealth.dtypes
#On passe notre variable target en float
airhealth['data_valuemessage'] = pd.to_numeric(airhealth['data_valuemessage'])
#Deprecated fonction mais qui avait l'air pas mal : airhealth = airhealth.convert_objects(convert_numeric=True)

# airhealth['Measure'].unique()

# airhealth['geo_type_name'].unique()
# airhealth[airhealth['geo_type_name'] == 'Citywide']

#airbonus = pd.read_csv('Datas/airbonus.csv', low_memory=False)

########################################
######## UTILISATION DE MONGODB ########
########################################

client = pymongo.MongoClient()
client.drop_database('test')
db = client.test
airMG = db.airMG
airMG.insert_many(airhealth.to_dict('records'))
# you need to pass the 'records' as argument in order to get a list of dict.
pprint(list(airMG.find()))

airhealth['indicator_id'].unique()



#Fonction qui me permet de réindexer les valeurs de toxicité entre 1 et 100
#pour les indicateurs choisis plus tard
def scale100(data):
    daata = data.drop(data.columns[[0,1]], axis=1)
    maxn = 100
    minn = 0
    maxo = daata.max()
    mino = daata.min()
    new = maxn-((maxn-minn)/(maxo-mino)*(daata-maxo)+maxn)
    return(pd.concat([data[[0,1]],new],axis = 1))

#Ici on va effectuer plusieurs pipelines dépendant de i (chaque indicateur_id choisi)
#On va append chaque résultat de mongodb à un pandas df
#puis on rescale les données entre 1 et 100
#on fait la moyenne de chaque indicateurs choisis et on obtient la meilleure location

scores = pd.DataFrame(columns = ['_id','quartier_id'])
for i in [639,640,641,642,643,644,645,646,647,657]:
	pipeline =[
	{ "$match": { "$and": [ {"geo_type_name":"UHF42"}, {"indicator_id":i}] } },
		{"$group": {
	          "_id" : "$geo_entity_name",
			  "quartier_id" : {"$first":"$geo_entity_id"},
	          "total" : {"$avg":"$data_valuemessage"}
	        }
	    }
	    ,{"$project" : {"total" : 1,"quartier_id":1 }}
	    ]
	tt = pd.DataFrame(list(airMG.aggregate(pipeline)))
	tt.rename(columns={'total':'ind'+str(i)}, inplace=True)
	scores = pd.merge(scores,tt,how='outer',on=['_id','quartier_id'])

scores100 = scale100(scores)
scores100['quartier_id'] = scores100['quartier_id'].apply(str)
scores100['average'] = scores100.mean(numeric_only=True, axis=1)
scores100[['_id','quartier_id','average']].sort('average',ascending = 0)
