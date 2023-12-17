import pandas as pd 
import geopandas as gpd 
from shapely import wkt 
from datetime import datetime

import STARTHER
import nvdbapiv3 
import overlapp
import nvdbgeotricks

import pickle

if __name__ == '__main__': 

    t0 = datetime.now()

    uoffisiell = False 
    if uoffisiell == True: 
        pNormal  = 't905_'
        pTommer  = 't901_'
        pSpesial = 't903_'
    else: 
        objTyper = [900, 902, 904]
        pNormal  = 't904_'
        pTommer  = 't900_'
        pSpesial = 't902_'

    bkdata = []
    for objType in objTyper: 
        sok = nvdbapiv3.nvdbFagdata( objType )
        sok.filter( {'kommune' : 5001 })
        temp = sok.to_records()
        bkdata.append( temp )


    vegnettSOK = nvdbapiv3.nvdbVegnett( filter={'kommune' : 5001, 
                                            'trafikantgruppe' : 'K', 'veglenketype' : 'hoved,konnektering', 
                                            'vegsystemreferanse' : 'E,R,F,K'}  )


    vegnett = pd.DataFrame( vegnettSOK.to_records() )
    vegnett['Gatenavn'] = vegnett['gate'].apply( lambda x : x['navn'] if isinstance( x, dict) and 'navn' in x else '' )    
    vegnett = gpd.GeoDataFrame( vegnett, geometry=vegnett['geometri'].apply( wkt.loads), crs=5973 )
    vegnettCol = [ 'fylke', 'kommune', 'vegkategori', 'fase', 'nummer', 'vref',  
                  'Gatenavn', 'feltoversikt', 'typeVeg', 'type' ]
    vegnettCol2 = [ 'adskilte_lop',  'lengde', 'trafikantgruppe',   
                    'veglenkesekvensid', 'startposisjon', 'sluttposisjon', 'detaljnivå', 
                    'geometry']
    vegnett = vegnett[vegnettCol+vegnettCol2].copy()


    tommer  = pd.DataFrame( bkdata[0])
    spesial = pd.DataFrame( bkdata[1])
    normal  = pd.DataFrame( bkdata[2])

    bkCol1 = ['Bruksklasse',  'Bruksklasse vinter' ]
    bkCol2 = [ 'Maks vogntoglengde', 'Strekningsbeskrivelse' ]
    bkKolALLE = bkCol1 + bkCol2
    tommerCol = ['Tillatt for modulvogntog 1 og 2 med sporingskrav' ] 
    spesialCol = ['Veggruppe' ]         
    idCol =     ['objekttype', 'nvdbId' ] 
    fellesCol = [ 'vref',   'veglenkesekvensid', 'startposisjon', 'sluttposisjon', 'segmentlengde', 'geometry']

    normal  = gpd.GeoDataFrame(  normal, geometry= normal['geometri'].apply( wkt.loads ), crs=5973 )
    tommer  = gpd.GeoDataFrame(  tommer, geometry= tommer['geometri'].apply( wkt.loads ), crs=5973 )
    spesial = gpd.GeoDataFrame( spesial, geometry=spesial['geometri'].apply( wkt.loads ), crs=5973 )

    joined1 = overlapp.finnoverlapp( vegnett,  normal[  bkKolALLE +              fellesCol + idCol ],  join='LEFT'  )
    joined2 = overlapp.finnoverlapp( joined1,  tommer[  bkKolALLE + tommerCol  + fellesCol + idCol ],  join='LEFT'  )
    joined3 = overlapp.finnoverlapp( joined2,  spesial[ bkKolALLE + spesialCol + fellesCol + idCol ],  join='LEFT' )

    joined3.sort_values( by=['kommune', 'vref'], inplace=True )
    joined3.reset_index()

    # Lager kolonner for pen presentasjon av datasett
    bruksklasseCol = []

    for kolonne in bkCol1: 
        for bkType in [ pNormal, pTommer, pSpesial ]: 
            bruksklasseCol.append( bkType +  kolonne )

    bruksklasseCol.extend( [ pTommer   + x for x in tommerCol] )
    bruksklasseCol.extend( [ pSpesial  + x for x in spesialCol] )
    for kolonne in bkCol2: 
        for bkType in [ pNormal, pTommer, pSpesial ]: 
            bruksklasseCol.append( bkType +  kolonne )
    for kolonne in idCol:
        for bkType in [ pNormal, pTommer, pSpesial ]: 
            bruksklasseCol.append( bkType + kolonne )

    joined3.fillna( '', inplace=True )

    avvik = joined3[ 
                        ( joined3[ bruksklasseCol[0] ] != joined3[ bruksklasseCol[1] ] ) |  # BK normal != BK tømmer
                        ( joined3[ bruksklasseCol[0] ] != joined3[ bruksklasseCol[2] ] ) |  # Bk normal != Bk spesial
                        ( joined3[ bruksklasseCol[3] ] != joined3[ bruksklasseCol[4] ] ) |  # Bk vinter, normal != BK vinter, tømmer
                        ( joined3[ bruksklasseCol[3] ] != joined3[ bruksklasseCol[5] ] )    # BK vinter, normal != BK vinter, spesial
                     ]

    avvik_fullutstrekning = joined3[   
        ( joined3[  pNormal + 'nvdbId' ].isin( avvik[  pNormal + 'nvdbId'] ) ) |
        ( joined3[  pTommer + 'nvdbId' ].isin( avvik[  pTommer + 'nvdbId'] ) ) |
        ( joined3[ pSpesial + 'nvdbId' ].isin( avvik[ pSpesial + 'nvdbId'] ) ) 
    ].copy()

    avvik_fullutstrekning.sort_values( by=[pNormal+'nvdbId', pTommer+'nvdbId', pSpesial+'nvdbId', 'kommune', 'vref' ], inplace=True )

    hull = joined3[ 
                    ( joined3[ pNormal  + 'nvdbId'] == '') | 
                    ( joined3[ pTommer  + 'nvdbId'] == '') | 
                    ( joined3[ pSpesial + 'nvdbId'] == '') 
                ]

    aggCol = ['vegkategori',  
                pNormal+'Bruksklasse', pTommer+'Bruksklasse', pSpesial+'Bruksklasse', 
                pNormal+'Bruksklasse vinter', pTommer+'Bruksklasse vinter', pSpesial+'Bruksklasse vinter',
                pTommer+'Tillatt for modulvogntog 1 og 2 med sporingskrav',
                pSpesial+'Veggruppe' ]
    statistikk = joined3.groupby( aggCol ).agg( {'lengde' : 'sum' } ).reset_index()

    t1 = datetime.now()
    metadata = pd.DataFrame( [{'Hva' : 'start', 'verdi' :  str(t0 )}, 
                              {'Hva' : 'slutt', 'verdi' : str(t1)}, 
                              {'Hva' : 'tidsbruk', 'verdi' : str(t1-t0)}, 
                                ]  )

    mineKolonner = vegnettCol + bruksklasseCol + vegnettCol2
    nvdbgeotricks.skrivexcel( 'bruksklasseOffisiell_analyse.xlsx', [ statistikk, avvik[mineKolonner], 
                                                               avvik_fullutstrekning[ mineKolonner ],
                                                               hull[ mineKolonner ],  metadata ], 
                             sheet_nameListe=[ 'Statistikk', 'Avvik', 'Avvik og hull', 'Hull', 'metadata' ])
    
    joined3[mineKolonner].to_file( 'AllOverlapp.gpkg', layer='Overlapp vegnett og BK offisiell', driver='GPKG')

    gpkgFil = 'bruksklasseOffisiell_analyse.gpkg'
    avvik[mineKolonner].to_file( gpkgFil, layer='Avvik', driver='GPKG')
    avvik_fullutstrekning[mineKolonner].to_file( gpkgFil, layer='Avvik og hull', driver='GPKG')
    hull[mineKolonner].to_file( gpkgFil, layer='Hull', driver='GPKG')
