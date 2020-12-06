import glob
import pandas as pd
import numpy as np


def read_csvfile(path):
    df = []
    count = 0
    for file in sorted(glob.iglob(path)):
        if count == 0:
            df = pd.read_csv(file, encoding = "ISO-8859-1", header = 0)
        else:
            df = df.append(pd.read_csv(file, encoding = "ISO-8859-1", header = 0))
        count+=1
    return df

def replace_column(df, column, val, new_val):
    df[column] = df[column].str.replace(val, new_val)

def trim_columns(df):
    for column in df:
        df[column] = df[column].str.strip()
    return df

def set_str(df, column):
    print(df[column])
    df[column] = df[column].astype(str) 

def set_int_str(df, column):
    df[column] = df[column].fillna(0)
    df[column] = df[column].astype(int).astype(str) 


def clean_logistic(df):
    replace_column(df, 'origen', 'ï¿½', ' ')
    replace_column(df, 'destino', 'ï¿½', ' ')
    replace_column(df, 'origen', chr(202), ' ')
    replace_column(df, 'destino', chr(202), ' ')
    replace_column(df, 'piloto', 'ï¿½', ' ')
    replace_column(df, 'piloto', chr(202), ' ')
    replace_column(df, 'piloto', '  ', ' ')
    set_int_str(df, 'placa_rastra')
    trim_columns(df)

def getWarehouses(df, index_start='origen', index_end='destino'):
    hq = (pd.concat([pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df[index_start])))]),
                    pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df[index_end])))])])
        .drop_duplicates()
        .rename(columns={0: 'Warehouses'})
        .sort_values(by='Warehouses')
        .reset_index(drop=True))
    return hq

def getRemisionesCases(df1, df2, df3):
    def partialDF(df, name):
        df = (pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df['remision'])))])
                 .rename(columns={0: 'remision'})
                 .set_index('remision'))
        df[name] = True
        return df
    
    def getCase(row):
        if (row['cocacola'] and row['logistic_water']) or (row['cocacola'] and row['logistic_soda']):
            return 'Case 1'
        elif (row['cocacola'] and ~row['logistic_water']) or (row['cocacola'] and ~row['logistic_soda']) :
            return 'Case 2'
        else:
            return 'Case 3'
    
    df1unique = partialDF(df1, 'cocacola')
    df2unique = partialDF(df2, 'logistic_water')
    df3unique = partialDF(df3, 'logistic_soda')
    
    dfList = [df1unique, df2unique, df3unique]
    df = pd.concat(dfList, axis=1, sort=False).fillna(False)
    df['case'] = df.apply(lambda x: getCase(x), axis=1)
                  
    return df



def getPilotos(df, df2):
    hq = (pd.concat([pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df['piloto'])))]),
                    pd.DataFrame([x.strip(' ') for x in sorted(set(map(str,df2['piloto'])))])])
        .drop_duplicates()
        .rename(columns={0: 'piloto'})
        .sort_values(by='piloto')
        .reset_index(drop=True))
    return hq


def cleanPilotos(df):
    def getPiloto(row):
        if row['piloto'] in drivers:
            return drivers[row['piloto']]
        else:
            return row['piloto']
        
    df['piloto_clean'] = df.apply(lambda x: getPiloto(x), axis=1)
    df.drop('piloto', axis=1, inplace=True)
    df.rename(columns={'piloto_clean':'piloto'}, inplace=True)

drivers = {'ALBERTO NAPOLEON VASQUEZ' : 'ALBERTO NAPOLEON VASQUEZ RAMIREZ',
                'ALBERTO  GUEVARA MARROQUIN' : 'ALBERTO GUEVARA MARROQUIN',
                'ANGEL ARNOLDO LARIOS ALVARENGA' : 'ANGEL ORLANDO LARIOS ALVARENGA',
                'ANGEL ORLANDO LARIOS' : 'ANGEL ORLANDO LARIOS ALVARENGA',
                'BORIS BIENVENIDO  PE A BENITEZ' : 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO  PE\x84A BENITEZ' : 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO PE\x84A BENITEZ' : 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO PE A BENITEZ': 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO PE\x84A': 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO PEÑA VENITEZ': 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO' : 'BORIS BIENVENIDO PENA VENITEZ',
                'BORIS BIENVENIDO PEÑA': 'BORIS BIENVENIDO PENA VENITEZ',
                'CARLOS ALONSO ESCOBARÊ RAMOS' : 'CARLOS ALONSO ESCOBAR RAMOS',
                'CARLOS ALONSO ESCOBAR  RAMOS' : 'CARLOS ALONSO ESCOBAR RAMOS',
                'CARLOS ALONSO RAMOS' : 'CARLOS ALONSO ESCOBAR RAMOS',
                'CARLOS ARNOLDO FLORES' : 'CARLOS ARNOLDO FLORES FLORES',
                'CARLOS ARNOLDO  FLORES FLORES' : 'CARLOS ARNOLDO FLORES FLORES',
                'CARLOS ARNULFO FLORES FLORES' : 'CARLOS ARNOLDO FLORES FLORES',
                'CARLOS LÎPEZ' : 'CARLOS LOPEZ',
                'CARLOS L PEZ' : 'CARLOS LOPEZ',
                'CARLOS LUIS ROMERO AYALAÊ': 'CARLOS LUIS ROMERO AYALA',
                'CARLOS ORLANDO FLORES' : 'CARLOS ARNOLDO FLORES FLORES',
                'CARLOS ORLANDO FLORES FLORES' : 'CARLOS ARNOLDO FLORES FLORES',
                'DAVID ANTONIO JIMENEZ' : 'DAVID ANTONIO JIMENEZ IRAHETA',
                'DAVID ANOTNIO JIMENEZ RAHETA' : 'DAVID ANTONIO JIMENEZ IRAHETA',
                'DAVID ANTONIO  JIMENEZ IRAHETA' : 'DAVID ANTONIO JIMENEZ IRAHETA',
                'DAVID ANTONIO JIMENEZ RAHETA' : 'DAVID ANTONIO JIMENEZ IRAHETA',
                'DAVID PARADA MARROQUIN' : 'DAVID MARROQUIN PARADA',
                'DAVID PARADA  MARROQUIN' : 'DAVID MARROQUIN PARADA',
                'DOUGLAS GONZALES CUELLAR' : 'DOUGLAS GONZALEZ CUELLAR',
                'DOUGLAS  GONZALES CUELLAR' : 'DOUGLAS GONZALEZ CUELLAR',
                'DOUGLAS GONZALEZ  CUELLAR' : 'DOUGLAS GONZALEZ CUELLAR',
                'DUGLAS GONZALEZ CUELLAR' : 'DOUGLAS GONZALEZ CUELLAR',
                'EDUARDO GABRIEL GUZMAN' : 'EDUARDO GABRIEL GUZMAN CENTENO',
                'EDUARDO MARTNEZ SANTOS' : 'EDUARDO MARTINEZ SANTOS',
                'EDWIN GARCêA TORRES' : 'EDWIN GARCIA TORRES',
                'EDQIN GARGIA TORRES' : 'EDWIN GARCIA TORRES',
                'EDWIN GARC A TORRES' : 'EDWIN GARCIA TORRES',
                'EDWIN GARGIA TORRES' : 'EDWIN GARCIA TORRES',
                'EDWIN VLADIMIR AVALOS' : 'EDWIN VLADIMIR AVALOS NUNES',
                'EDWIN VLADIMIR AVALOS NU\x84ES' : 'EDWIN VLADIMIR AVALOS NUNES',
                'EUGENIO GONZALEZ VILLA NUEVA' : 'EUGENIO GONZALES VILLANUEVA',
                'EUGENIO GONZALEZ VILLANUEVA' : 'EUGENIO GONZALES VILLANUEVA',
                'EVER JOSE ARGUMEDO' : 'EVER JOSUE ARGUMEDO ESCALON',
                'EVER JOSUE ARGUMEDO' : 'EVER JOSUE ARGUMEDO ESCALON',
                'FRANCISCO ALBERTO MONGE' : 'FRANCISCO ALBERTO MONGE SEGURA',
                'FRANCISCO ANTONIO MONGE SEGURA' : 'FRANCISCO ALBERTO MONGE SEGURA',
                'FRANCISCO AVILÉS' : 'FRANCISCO AVILES',
                'FRANCISCO AVILEZ' : 'FRANCISCO AVILES',
                'FRANCISCO AVIL S' : 'FRANCISCO AVILES',
                'FREDI REYNALDO SUBVADO MARTINEZ' : 'FREDY REYNALDO SUBNADO MARTINEZ',
                'FREDY REYNALDO SUBVADO MARTINEZ' : 'FREDY REYNALDO SUBNADO MARTINEZ',
                'GERMBERT VASQUEZ' : 'GEMBERT MAURICIO BASQUEZ ALFARO',
                'GERMAN ANGEL CARPIO' : 'GERMAN ANGEL CARPIO SANCHEZ',
                'GERMAN CARPIO' : 'GERMAN ANGEL CARPIO SANCHEZ',
                'GERMAN GARCIA' : 'GERMAN ARMANDO GARCIA',
                'GERMAN VIDAL RAMREZ RVAS' : 'GERMAN VIDAL RAMIREZ RIVAS',
                'GERMAN VIDAL RIVAS': 'GERMAN RAMIRES RIVAS' ,
                'GREGORIO DE JESUS TRUJILLO' : 'GREGORIO DE JESUS TRUJILLO ZETINO',
                'HERIBERTO ALFONSO RUBIO' : 'HERIBERTO ALFONSO RUBIO MOLINA',
                'HERIBERTO ALONSO RUBIO' : 'HERIBERTO ALFONSO RUBIO MOLINA',
                'ISAêAS OSWALDO BURGOS AQUINO' : 'ISAIAS OSWALDO BURGOS AQUINO',
                'ISA AS OSWALDO BURGOS AQUINO' : 'ISAIAS OSWALDO BURGOS AQUINO',
                'JAIME  SALVADOR TREJO MARAVILLA' : 'JAIME SALVADOR MARAVILLA TREJO',
                'JAIME SALVADOR   MARAVILLA TREJO' : 'JAIME SALVADOR MARAVILLA TREJO', 
                'JAIME SALVADOR  MARAVILLA TREJO' : 'JAIME SALVADOR MARAVILLA TREJO', 
                'JAIME SALVADOR TREJO MARAVILLA' : 'JAIME SALVADOR MARAVILLA TREJO', 
                'JAIME SALVADOR TREJO': 'JAIME SALVADOR MARAVILLA TREJO', 
                'JAVIER EXSAU GONZALEZ' : 'JAVIER EXSAU GONZALEZ LOPEZ',
                'JAVIER GONZALEZ': 'JAVIER EXSAU GONZALEZ LOPEZ',
                'JEFFERSON OMAR  CRESPIN' : 'JHEFFERSON OMAR CRESPIN PERALTA',
                'JHEFERSON OMAR CRESPIN' : 'JHEFFERSON OMAR CRESPIN PERALTA',
                'JHEFFERSON OMAR CRESPIN' : 'JHEFFERSON OMAR CRESPIN PERALTA',
                'JHEFFERSON OMAR CRESPIN PERALTAÊ' : 'JHEFFERSON OMAR CRESPIN PERALTA',
                'JORGE  ANTONIO CABRERA' : 'JORGE ANTONIO CABRERA', 
                'JORGE ANTONIO  CABRERA' : 'JORGE ANTONIO CABRERA',
                'JOSE ALFREDO  RENDEROS' : 'JOSE ALFREDO RENDEROS',
                'JOSE ALFREDO VALENCA QUI\x84ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QUI\x84ONES' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QUI\x84ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QU\x84ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCA QUI ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QU ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QUI ONES' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QUI ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ALFREDO VALENCIA QUI\x84ONEZ' : 'JOSE ALFREDO VALENCIA QUINONEZ',
                'JOSE ANTONIO  GUEVARA' : 'JOSE ANTONIO GUEVARA',
                'JOSE FRANCISCO  CANIZALEZ' : 'JOSE FRANCISCO CANIZALEZ MARTINEZ',
                'JOSE FRANCISCO CANIZALES' : 'JOSE FRANCISCO CANIZALEZ MARTINEZ',
                'JOSE FRANCISCO CANIZALES MARTINEZ' : 'JOSE FRANCISCO CANIZALEZ MARTINEZ',
                'JOSE FRANCISCO CANIZALEZ' : 'JOSE FRANCISCO CANIZALEZ MARTINEZ',
                'JOSE ISAEL IGLESIA': 'JOSE ISAEL IGLESIAS',
                'JOSE ISMAEL FLORES': 'JOSE ISMAEL FLORES MELGAR',
                'JOSE LUIS  HERNANDEZ':'JOSE LUIS HERNANDEZ MARROQUIN',
                'JOSE LUIS HERNANDEZ':'JOSE LUIS HERNANDEZ MARROQUIN',
                'JOSE LUIS HERNANDEZ MAROQUIN':'JOSE LUIS HERNANDEZ MARROQUIN',
                'JOSE NAPOLEON GARCIA' : 'JOSE NAPOLEON GARCIA TORRES',
                'JOSE OSCAR MARTINEZ' : 'JOSE OSCAR MARTINEZ GIRON',
                'JOSE OSCAR MARTINEZ JIRON' : 'JOSE OSCAR MARTINEZ GIRON',
                'JOSE REINALDO SELAYA' : 'JOSE REINALDO ZELAYA',
                'JOS\x83 REYNALDO ZELAYA' : 'JOSE REINALDO ZELAYA',
                'JOS\x83 VêCTOR MORALES ORANTES' : 'JOSE VICTOR MORALES ORANTES',
                'JOS V CTOR MORALES ORANTES' : 'JOSE VICTOR MORALES ORANTES',
                'JUAN ANTONIO MENJIVAR' : 'JUAN ANTONIO MENJIVAR MARTINEZ',
                'JUAN ANTONIO MENJIVAR MARTINEZÊ' : 'JUAN ANTONIO MENJIVAR MARTINEZ',
                'JUAN ANTONIO SANCHEZ' : 'JUAN ANTONIO SANCHEZ RIVERA',
                'JUAN ANTONIO SANCHEZ RIVERAÊ' : 'JUAN ANTONIO SANCHEZ RIVERA',
                'JUAN ANTONIO SçNCHEZ RIVERA' : 'JUAN ANTONIO SANCHEZ RIVERA',
                'JUAN ANTONIO S NCHEZ RIVERA' : 'JUAN ANTONIO SANCHEZ RIVERA',

                'JUAN CARLOS  TORRES': 'JUAN CARLOS TORRES ORELLANA',
                'JUAN CARLOS  TORRES ORELLANA': 'JUAN CARLOS TORRES ORELLANA',
                'JUAN CARLOS TORES': 'JUAN CARLOS TORRES ORELLANA',
                'JUAN CARLOS TORRES': 'JUAN CARLOS TORRES ORELLANA',
                'JUAN CARLOS TORRES GALDAMES': 'JUAN CARLOS TORRES ORELLANA',
                'JUAN CARLOS GARCIA' : 'JUAN CARLOS GARCIA MORAN',
                'JUAN CARLOS GARCIA MORANÊ' : 'JUAN CARLOS GARCIA MORAN',
                'JUAN CARLOS MENDEZ' : 'JUAN CARLOS MENDEZ RAMIREZ',
                'JUAN CARLOS MENDEZ  RAMIREZ' : 'JUAN CARLOS MENDEZ RAMIREZ',
                'JUAN CARLOS MENDEZÊ RAMIREZ' : 'JUAN CARLOS MENDEZ RAMIREZ',
                'JUAN CARLOS M NDEZ RAM REZ' : 'JUAN CARLOS MENDEZ RAMIREZ',

           
                'JUAN CARLOS M\x83NDEZ RAMêREZ' : 'JUAN CARLOS MENDEZ RAMIREZ',
                'JUAN JOSE  REYES' : 'JUAN JOSE REYES AGUILERA',
                'JUAN JOSE  REYES AGUILERA' : 'JUAN JOSE REYES AGUILERA',
                'JUAN JOSE REYES' : 'JUAN JOSE REYES AGUILERA',
                'JUAN JOSE REYES AGUILERAÊ' : 'JUAN JOSE REYES AGUILERA',
                'JULIO CESAR  MARTINEZ' : 'JULIO CESAR MARTINEZ',
                'JULIO CESAR MARTINEZÊ' : 'JULIO CESAR MARTINEZ',
                'JULIO CESAR VASQUEZ' : 'JULIO CESAR VASQUEZ RAMOS',
                'LUIS GERARDO GUARADADO MONJARAS' : 'LUIS GERARDO GUARDADO MONJARAS',
                'LUIS GERARDO GUARDADO' : 'LUIS GERARDO GUARDADO MONJARAS',
                'MARIO ANTONIO  RIVERA': 'MARIO ANTONIO RIVERA RAMIREZ',
                'MARIO ANTONIO RAMIREZ RIVERA': 'MARIO ANTONIO RIVERA RAMIREZ',
                'MARIO ANTONIO RIVERA': 'MARIO ANTONIO RIVERA RAMIREZ',
                'MARIO ANTONIO RIVERA RAMREZ': 'MARIO ANTONIO RIVERA RAMIREZ',
                'MARIO ANTONO RIVERA RAMREZ': 'MARIO ANTONIO RIVERA RAMIREZ',
                'MAURICIO ANTONIO SOLIS' : 'MAURICIO ANTONIO SOLIS CHILIN',
                'MAURICIO ERNESTO VçSQUEZ CHçVEZ' : 'MAURICIO ERNESTO VASQUEZ CHAVEZ',
                'MAURICIO ERNESTO V SQUEZ CH VEZ' : 'MAURICIO ERNESTO VASQUEZ CHAVEZ',
                'MIGUEL  ANGEL URQUILLA' : 'MIGUEL ANGEL URQUILLA ZEPEDA',
                'MIGUEL ANGEL URQUILLA' : 'MIGUEL ANGEL URQUILLA ZEPEDA',
                'MIGUEL ANGEL  PINEDA' : 'MIGUEL ANGEL PINEDA PLATERO',
                'MIGUEL ANGEL  PINEDA PLATERO' : 'MIGUEL ANGEL PINEDA PLATERO',
                'MIGUEL ANGEL PINEDA' : 'MIGUEL ANGEL PINEDA PLATERO',
                'MIGUEL ANGELÊ PINEDA PLATEROÊ' : 'MIGUEL ANGEL PINEDA PLATERO',
                'MIGUEL ANGUEL PINEDA PLATERO' : 'MIGUEL ANGEL PINEDA PLATERO',
                'MIGUEL ANGEL RIVERA' : 'MIGUEL ALBERTO RIVERA',
                'MIGUEL ALBERTO RIVERA Ê' : 'MIGUEL ALBERTO RIVERA',
                'MIGUEL ERNESTO BERMUDESZ BARRIENTO' : 'MIGUEL ERNESTO BERMUDEZ BARRIENTOS',
                'MIGUEL ERNESTO BERMUDEZ BARRIENTO' : 'MIGUEL ERNESTO BERMUDEZ BARRIENTOS',
                'NEFTALI ALEXANDER  MOYA' : 'NEFTALI ALEXANDER MOYA',
                'NEFTALY ALEXANDER  MOYA' : 'NEFTALI ALEXANDER MOYA',
                'NEFTALY ALEXANDER MOYA' : 'NEFTALI ALEXANDER MOYA',
                'NERIS AMADO ARTEAGA RAMIRES' : 'NERIS AMADO ARTEAGA MARTINEZ',
                'NERIS AMADO ARTIAGA  MARTINEZ' : 'NERIS AMADO ARTEAGA MARTINEZ',
                'NERIS AMADO MARTINEZ' : 'NERIS AMADO ARTEAGA MARTINEZ',
                'NERIS AMADO MARTINEZ ARTIAGA' : 'NERIS AMADO ARTEAGA MARTINEZ',
                'NERIS AMADO ARTIAGA MARTINEZ' : 'NERIS AMADO ARTEAGA MARTINEZ',
                'NICOLAS ISMAEL  REYES' : 'NICOLAS ISMAEL REYES GUZMAN',
                'NICOLAS ISMAEL  REYES GUZMAN' : 'NICOLAS ISMAEL REYES GUZMAN',
                'NICOLAS ISMAEL REYES' : 'NICOLAS ISMAEL REYES GUZMAN',
                'NOE ALEXANDER ARDON' : 'NOE ALEXANDER ARDON MORALES',
                'ORESTES BIENVENIDO TRIGUEROS' : 'ORESTES BIENVENIDO TRIGUEROS AGUILAR',
                'OSCAR ARMAMDO  GARCIA MARTINEZ' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR ARMAMDO GARCIA MARTINEZ' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR ARMANDO  GARCIA' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR ARMANDO  GARCIA MARTINEZ' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR ARMANDO GARCIA' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR ARMANDO GARCIA  MARTINEZ' : 'OSCAR ARMANDO GARCIA MARTINEZ',
                'OSCAR NOE CARDOZA' : 'OSCAR NOE CARDOZA CABRERA',
                'PABLO ANTONIO RODRêGUEZ CHEVEZ' : 'PABLO ANTONIO RODRIGUEZ CHEVEZ',
                'PABLO ANTONIO RODR GUEZ CHEVEZ' : 'PABLO ANTONIO RODRIGUEZ CHEVEZ',
                'RAFAEL ARMANDO  CANALES' : 'RAFAEL ARMANDO CANALES PEREZ',
                'RAFAEL ARMANDO CANALES' : 'RAFAEL ARMANDO CANALES PEREZ',
                'RAFAEL ARMANDO CANALEZ' : 'RAFAEL ARMANDO CANALES PEREZ',
                'RENE ARMANDO  DIAZ MELGAR' : 'RENE ARMANDO DIAZ MELGAR',
                'RENE ARMANDO DIAZ' : 'RENE ARMANDO DIAZ MELGAR',
                'RONALD MEDRANO DEL VALLE CRUZ' : 'RONALD MEDARDO DEL VALLE CRUZ',
                'RONALD MEDRANO' : 'RONALD MEDARDO DEL VALLE CRUZ',
                'RONALD MEDARDO DE LA CRUZ' : 'RONALD MEDARDO DEL VALLE CRUZ',
                'RONAL MEDARDO DEL VALLE CRUZ' : 'RONALD MEDARDO DEL VALLE CRUZ',
                'RONAL MEDARDO DEL VALLE' : 'RONALD MEDARDO DEL VALLE CRUZ',
                'WALTER MISAEL PE\x84A' : 'WALTER MISAEL PENA',
                'WALTER MISAEL PE A' : 'WALTER MISAEL PENA',
                'WALTER ROBERTO SANCHEZ' : 'WALTER ROBERTO SANCHEZ ALVARADO',
                'WILLIAN BALMORE CRUZ' : 'WILLIAM BALMORE CRUZ BARRIENTOS',
                'WILLIAM BALMORE CRUZ' : 'WILLIAM BALMORE CRUZ BARRIENTOS',
                'WILIAN BALMORE CRUZ' : 'WILLIAM BALMORE CRUZ BARRIENTOS',
                'WILLIAN BALMORE CRUZ BARRIENTOS' : 'WILLIAM BALMORE CRUZ BARRIENTOS',
                'WILLIAM ALEXANDER  MELARA LANDAVERDE' : 'WILLIAM ALEXANDER MELARA LANDAVERDE',
                'WILLIAN ALEXANDER MELARA' : 'WILLIAM ALEXANDER MELARA LANDAVERDE',
                'WILLIAN ALEXANDER MELARA LANDA VERDE' : 'WILLIAM ALEXANDER MELARA LANDAVERDE',
                'WILLIAN ALEXANDER MELARA LANDAVERDE' : 'WILLIAM ALEXANDER MELARA LANDAVERDE'
             }

def tableauCSV(df1, df2, df3, remisiones_df):
    def toAppendCase1(rm, df1, df2, dep, df):
        line = df1.loc[df1['remision'] == rm]
        piloto = df2['piloto'].loc[df2['remision'] == rm]
        if len(piloto) > 0:
            try:
                piloto = piloto.item()
            except ValueError:
                piloto = piloto.iloc[0]
        else:
            piloto = np.nan
        return (df.append({'remision':rm, 'case':'Case 1', 'fecha':line['fecha'].item(),
                           'origen':line['origen'].item(), 'destino':line['destino'].item(), 
                                  'kilometros':line['kilometros'].item(), 'piloto':piloto,
                           'department':str(dep)}, ignore_index=True))
    def toAppendCase2(rm, df1, df):
        line = df1.loc[df1['remision'] == rm]
        return (df.append({'remision':rm, 'case':'Case 2', 'fecha':line['fecha'].item(),
                           'origen':line['origen'].item(), 'destino':line['destino'].item(), 
                           'kilometros':line['kilometros'].item(), 'piloto':np.nan,
                           'department':np.nan}, ignore_index=True))

    def toAppendCase3(rm, df1, dep, df):
        line = df1.loc[df1['remision'] == rm]

        if len(line) > 1:
            temp = pd.DataFrame(columns=['remision', 'fecha', 'case', 'origen', 'destino',
                                         'kilometros', 'piloto', 'department'])
            for index, row in line.iterrows():
                temp = temp.append({'remision':rm, 'case':'Case 3', 
                                    'fecha':row['fecha'],
                                    'origen':row['origen'],
                                    'destino':row['destino'], 
                                    'kilometros':row['kilometros'],
                                    'piloto':row['piloto'],
                                    'department':dep}, ignore_index=True)
            return df.append(temp, ignore_index=True)

        elif len(line) == 1: 
            return (df.append({'remision':rm, 
                               'case':'Case 3', 
                               'fecha':line['fecha'].item(), 
                               'origen':line['origen'].item(), 
                               'destino':line['destino'].item(),
                               'kilometros':line['kilometros'].item(),
                               'piloto':line['piloto'].item(),
                               'department':dep},
                              ignore_index=True))
        return df

    tableau = pd.DataFrame(columns=['remision',
                                    'case',
                                    'fecha',
                                    'origen',
                                    'destino',
                                    'kilometros',
                                    'piloto',
                                    'department'])
    
    tableau.piloto = tableau.piloto.str.encode("ISO-8859-1")

    for index, row in remisiones_df.iterrows():
        if row['case'] == 'Case 1':
            if row['logistic_water']:
                tableau = toAppendCase1(index, df1, df2, 'WATER', tableau)
            elif row['logistic_soda']:
                tableau = toAppendCase1(index, df1, df3, 'SODA', tableau)
        elif row['case'] == 'Case 2':
            tableau = toAppendCase2(index, df1, tableau)
        elif row['case'] == 'Case 3':
            if row['logistic_water']:
                tableau = toAppendCase3(index, df2, 'WATER', tableau)
            elif row['logistic_soda']:
                tableau = toAppendCase3(index, df3, 'SODA', tableau)
            #print(index," ", row)
    return tableau

def clean_tableau(df):
    replace_column(df, 'piloto', '  ', ' ')
    replace_column(df, 'origen', '  ', ' ')
    replace_column(df, 'destino', '  ', ' ')
    replace_column(df, 'remision', '  ', ' ')
    trim_columns(df)