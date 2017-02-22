#############################################################################
########################### Crimes parcs New York ###########################
#############################################################################

from pprint import pprint #pour afficher les résultats des curseurs mongodb
import pandas as pd
import pymongo

#####################################
######### IMPORT DE LA BASE #########
#####################################
## 2 : Crimes commis dans les parcs de New York
crimes = pd.read_excel('./crimes.xlsx',skiprows = 4)

crimes[crimes.isnull().any(axis=1)]
crimes.ix[1154,2] = crimes['SIZE (ACRES)'].sum()
crimes.ix[1154,1] = 'NY'
crimes.ix[1154,3] = 'Total'

# crimes.fillna(value = 'TOTAL',inplace = True)

crimes.dtypes
#Quartiers mal remplis dans la base
orep = crimes['BOROUGH'].unique()
#Quartiers réécrits plus proprement
nrep = ['Bronx','Queens','Staten Island','Brooklyn','Brooklyn/Queens','Manhattan','Total']
#On applique la nouvelle mise en forme des boroughs
crimes['BOROUGH'].replace(orep,nrep,inplace=True)

########################################
######## UTILISATION DE MONGODB ########
########################################

client = pymongo.MongoClient()
# client.drop_database('test')
db = client.test
# db.crimesMG.drop()
crimesMG = db.crimesMG
crimesMG.insert_many(crimes.to_dict('records'))
# you need to pass the 'records' as argument in order to get a list of dict.
pprint(list(crimesMG.find()))
pipeline =[
		{"$group": {
	          "_id" : "$BOROUGH",
	          "kills" : {"$sum":"$TOTAL"},
              "area" : {"$sum":"$SIZE (ACRES)"},
              "parcs":{"$sum":1}
	        }
	    }
	    ,{"$project" : {"parcs":1,"kills" : 1,"area":1,
        "ratio":{ "$divide": [ "$area", "$kills" ] }}},
        {"$sort":  {"ratio" : -1} }
	    ]
pprint(list(crimesMG.aggregate(pipeline)))
