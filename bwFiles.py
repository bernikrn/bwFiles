import pandas as pd
import numpy as np
from datetime import datetime
import re
import gspread
import multiprocessing


class ParseTweets:
    '''
    Carga ficheros de Brandwatch, convierte caractes especiales
    '''

    def __init__(self, dictionary: dict):
        self.dictionary = dictionary


    def caracteres(self, dataframe: pd.DataFrame, column:str) -> pd.DataFrame:
        '''
        Convierte contenido de tweets a minusculas y elimina acentos
        '''
        dataframe[column] = dataframe[column].str.lower()
        dataframe[column].replace(self.dictionary, regex=True, inplace=True)
        return dataframe


    def cargaFicheroBW(self, path: str, columnsToLoad: list, geaFile=False) -> pd.DataFrame:
        '''
        carga ficheros .csv o .zip de BW
        realiza tratamiendo de datos y creacion de columna de semana en funcion
        de la extension que tenga el fichero a cargar
        '''
        if geaFile:
            if path[-4:] == '.zip':
                dataframe = pd.read_csv(path, usecols=columnsToLoad) # carga .csv
                dataframe.columns = map(str.lower, dataframe.columns)  # titulos en minusculas
                dataframe.columns = dataframe.columns.str.replace('aapp - ', '') # elimina prefijo
                dataframe['date'] = pd.to_datetime(dataframe['date']) # convierte de str a fecha
            else:
                raise ValueError('El fichero de gea debe estar en formato .zip')
            return dataframe
        
        if path[-4:] == '.csv':
            # fichero .csv ya tiene columna de semana
            dataframe = pd.read_csv(path, usecols=columnsToLoad)
            dataframe['date'] = pd.to_datetime(dataframe['date'])
        elif path[-4:] == '.zip':
            dataframe = pd.read_csv(path, usecols=columnsToLoad, skiprows=6) # carga .csv
            dataframe.columns = map(str.lower, dataframe.columns)  # titulos en minusculas
            dataframe.columns = dataframe.columns.str.replace('aapp - ', '') # elimina prefijo
            dataframe['date'] = pd.to_datetime(dataframe['date']) # convierte de str a fecha
        else:
            raise ValueError('El fichero a cargar debe ser .csv o .zip')

        #dataframe.sort_values(by=["semana"], inplace=True)
        return dataframe


    def reemplazarXpor1(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        '''
        Reemplaza 'X' por 1 y NaN por 0
        '''
        dataframe.replace('X', 1, inplace=True)
        dataframe.fillna(0, inplace=True)
        return dataframe



class ManipulacionDatos:
    ''' Manipulacion de dataframes, para asignar comunidades, crear columns 
    y filtrar
    '''
    def communityAssignation(
        self, dataframe: pd.DataFrame, communities: list, path:str
    ) -> pd.DataFrame:
        ''' Agrega la columna "community" al dataframe y asigna una de las 
        comunidades de la lista, para ello comprueba si el author se encuentra 
        en el fichero que contiene a todos los autores de esa comunidad
        '''
        df_final = pd.DataFrame()
        for community in communities:
            nombre_archivo = 'unique_authors_' + community + '.txt'
            
            with open(path + nombre_archivo, 'r') as file:
                palabras = file.read()
                list_authors = palabras.split(", ")
                df_comm = dataframe[dataframe["author"].isin(list_authors)]
                df_comm["community"] = community
            df_final = df_final.append(df_comm)
        return df_final

    def filtroDF(
        self, dataframe: pd.DataFrame, column: str, filtro: list) -> pd.Series:
        ''' Recive dataframe, la columna donde se buscaran las palabras clave y 
        lista de palabras a filtrar devuelve mascara para ser usada como filtro.
        '''
        # No se tienen en cuenta mayusculas, pero si las tildes
        return dataframe[column].str.contains(
            '|'.join(filtro), regex=True, flags=re.IGNORECASE)

    
    def parallel_filtroDF(self, dataframe: pd.DataFrame, column: str, filtro: list) -> pd.Series:
        num_processes = multiprocessing.cpu_count()  # Get the number of available CPU cores
        pool = multiprocessing.Pool(processes=num_processes)
        chunk_size = len(dataframe) // num_processes  # Split the dataframe into chunks

        # Apply the filtroDF method in parallel on each chunk of the dataframe
        results = pool.starmap(self.filtroDF, [(dataframe.iloc[i:i+chunk_size], column, filtro) for i in range(0, len(dataframe), chunk_size)])

        pool.close()
        pool.join()

        # Concatenate the results and return the final series
        return pd.concat(results)


    def creaColumnaTema(
        self, dataframe: pd.DataFrame, columnName: str,
        dataframeFiltrado: pd.DataFrame
    ) -> pd.DataFrame:
        ''' Crea una columna con un tema nuevo en el dataframe, si determinada
        fila ha pasado el filtro definido previamente, se le asiga valor 'X', 
        en caso contrario tendra el valor NaN
        '''
        #dataframe[columnName] = np.nan
        dataframe[columnName] = 0
        #dataframe.loc[dataframeFiltrado.index, columnName] = 'X'
        dataframe.loc[dataframeFiltrado.index, columnName] = '1'
        #conversion de tipo
        dataframe = dataframe.astype({columnName: np.uint8})

        return dataframe


    def creaColumnaTemaSimple(
        self, dataframe: pd.DataFrame, colFiltrar: str, nombreCol: str, 
        listaFiltro: list
    ) -> pd.DataFrame:
        ''' Agrega columna de tema nuevo, para filtro que solo contenga ORs
        '''
        filtro = self.filtroDF(dataframe, colFiltrar, listaFiltro)
        frases_filtro = dataframe[filtro]
        return self.creaColumnaTema(dataframe, nombreCol, frases_filtro)
    

    def parallel_creaColumnaTemaSimple(
        self, dataframe: pd.DataFrame, colFiltrar: str, nombreCol: str, 
        listaFiltro: list
    ) -> pd.DataFrame:
        ''' Agrega columna de tema nuevo, para filtro que solo contenga ORs
        '''
        filtro = self.parallel_filtroDF(dataframe, colFiltrar, listaFiltro)
        frases_filtro = dataframe[filtro]
        return self.creaColumnaTema(dataframe, nombreCol, frases_filtro)



class GsheetsFile:
    '''
    Metodos para modificar documentos Gsheets
    '''
    def selectWorksheet(
        self, name: str, sheet: gspread.models.Spreadsheet
        ) -> gspread.models.Worksheet:
        ''' Selecciona hoja de documento gsheets. Si no existe, la crea
        '''
        try:
            worksheet = sheet.worksheet(name)
        except gspread.WorksheetNotFound:
            print(f'worksheet does not exist. Creating worksheet: {name}')
            worksheet = sheet.add_worksheet(title=name, rows=200, cols=100)
        return worksheet

    
    def sobrescribir(
        self, worksheet, dataframe: pd.DataFrame, title:str=None, index=False,
            sysdate=False
        ):
        ''' Borra todo el contenido de la worksheet de Gsheets y escribe todos los datos del dataframe.
        El valor de index debe ser True si el dataframe tiene indice. El indice se crea al aplicar
        el metodo .groupby
        title: descripcion a ser insertada en la primer fila
        '''
        worksheet.clear()
        if title is not None:
            worksheet.append_rows([[title]])
        if index:
            header = [dataframe.reset_index().columns.values.tolist()]
        else:
            header = [dataframe.columns.values.tolist()]
        if sysdate:
            header[0].append('sysdate')

        worksheet.append_rows(header)
        return self.append(worksheet, dataframe, index=index, sysdate=sysdate)


    def append(
        self, worksheet, dataframe: pd.DataFrame, index=False, sysdate=False
        ):
        '''
        Todos los datos del dataframe, debajo de los datos que ya existan
        El valor de index debe ser True si el dataframe tiene indice. El indice se crea al aplicar
        el metodo .groupby.
        sysdate = True crea una columna con la fecha en la que fue insertado el dato en sheets
        '''
        if index:
            toAppend = dataframe.reset_index().values.tolist()
            if sysdate:
                now = datetime.now()
                return worksheet.append_rows([row + [now.strftime("%d/%m/%Y %H:%M:%S")] for row in toAppend])
            return worksheet.append_rows(toAppend)

        toAppend = dataframe.values.tolist()
        if sysdate:
            now = datetime.now()
            return worksheet.append_rows([row + [now.strftime("%d/%m/%Y %H:%M:%S")] for row in toAppend])

        return worksheet.append_rows(toAppend)



class ReportesTweets:
    '''
    Metodos para crear dataframes que seran escritos en gsheets
    '''
    def activacionComunidadSemana(
        self, dataframe: pd.DataFrame, comunidades: dict, path:str
        ) -> pd.DataFrame:
        '''
        Activacion de comunidades
        Porcenje de miembros de una comunidad que han generado al menos un mensaje, por semana
        '''
        # calcula miembros totales de cada comunidad
        usuariosComunidades = []
        for comunidad in list(comunidades.values()):
            usuariosComunidades.append(self._contarAutoresComunidad(path + 'unique_authors_' + comunidad + '.txt'))

        listaOut = [list(comunidades.keys())]
        # itera sobre cada semana
        for semana in dataframe['semana'].unique():
            listaSemana=[semana]
            dfSemanaN = dataframe[dataframe['semana'] == semana]
            # itera sobre cada comunidad
            for comunidad, usuarios in zip(list(comunidades.values()), usuariosComunidades):
                dfComunidad = dfSemanaN[dfSemanaN['community'] == comunidad]
                listaSemana.append( len(dfComunidad['author'].unique()) / usuarios)
            listaOut.append(listaSemana)

        listaOut[0].insert(0,'semana')

        return  pd.DataFrame(listaOut[1:],columns=listaOut[0:1][0])


    def evolucionConversacion(
        self, dataframe: pd.DataFrame, temasFavorables: list, temasDesfavorables: list, temasNeutrales: list, graficarNeutrales=False
        ) -> pd.DataFrame:
        '''
        Evolucion de la conversacion electoral
        Porcentaje de mensajes favorables y desfavorables sobre el total de mensajes, por semana
        '''
        temas = temasFavorables + temasDesfavorables + temasNeutrales

        # filtra solo las columnas de temas indicados en el argumento y la columna semana
        # y agrega por semana, sumando cantidad de comentarios por tema
        dataframe = self._reemplazarXpor1(dataframe.loc[:, temas+['semana'] ]).groupby('semana').sum()

        titulosColumnas = ['temas favorables', 'temas desfavorables' ]
        dataframe[titulosColumnas[0]] = dataframe[temasFavorables].sum(axis=1) / dataframe[temas].sum(axis=1)
        dataframe[titulosColumnas[1]] = dataframe[temasDesfavorables].sum(axis=1) / dataframe[temas].sum(axis=1)
        if len(temasNeutrales) > 0:
            titulosColumnas += ['temas neutrales']
            dataframe[titulosColumnas[2]] = dataframe[temasNeutrales].sum(axis=1) / dataframe[temas].sum(axis=1)

        if graficarNeutrales:
            return dataframe.loc[:,titulosColumnas]
        return dataframe.loc[:,titulosColumnas[0:2]]


    def tematicasInteres(
        self, dataframe: pd.DataFrame, temasFavorables: list, comunidades: dict, path:str=None
        ) -> pd.DataFrame:
        '''
        Porcentaje de mensajes de comunidad progesista y conservadora que ha hablado de un tema
        Si se especifica un path, calcula porcentaje de equilibrio, de la primerca comunidad
        contando la cantidad de autores de cada comunidad en los ficheros
        '''
        # filtra solo las columnas de temas y semana
        dataframe = dataframe.loc[:,temasFavorables + ['community']]

        # suma la cantidad de mensajes que cada comunidad ha genrado, por cada tema
        dataframe = dataframe[dataframe['community'].isin(list(comunidades.values()))]

        dataframe = self._reemplazarXpor1(dataframe).groupby('community').sum()
        dataframe.loc['Total']= dataframe.sum()

        # divide la cantidad de mensajes de cada comunidad por la cantidad de mensajes de todas las comunidades
        for tema in temasFavorables:
            dataframe[tema] = dataframe[tema] / dataframe.loc['Total', tema]

        # renombra las comunidades
        for nameKey, nameValue in comunidades.items():
            dataframe.loc[nameKey] = dataframe.loc[nameValue]
        
        if path is not None:
            # Cantidad total de authores
            totalComunidades = 0
            for comunidad in comunidades.values():
                pathComunidad = path + 'unique_authors_' + comunidad + '.txt'
                totalComunidades += self._contarAutoresComunidad(pathComunidad)
            
            # porcentaje de autores de la primer comunidad sobre total
            equilibrioC10001 = self._contarAutoresComunidad(path + 'unique_authors_' + list(comunidades.values())[0] + '.txt')
            
            dataframe.loc['equilibrio'] = equilibrioC10001 / totalComunidades

            return dataframe.loc[list(comunidades.keys()) + ['equilibrio'],]

        return dataframe.loc[list(comunidades.keys()) ,]

    def activacionComunidadTotal(
        self, dataframe: pd.DataFrame, comunidades: dict, temas:list, path:str
    ) -> pd.DataFrame:
        '''
        Porcentaje de activacion de total y de cada comunidad,
        de cada tema
        '''
        # calcula miembros totales de cada comunidad
        usuariosComunidades = []
        for comunidad in list(comunidades.values()):
            usuariosComunidades.append(self._contarAutoresComunidad(path + 'unique_authors_' + comunidad + '.txt'))
       
        listaTema = [ ]
        # itera sobre cada tema
        for tema in temas:
            # filtra filas solo de un tema
            dfTema = dataframe[dataframe[tema] == 'X']

            listaComunidad = [tema]
            # columna de autores activos en todas las comunidades
            dfComunidad = dfTema[dfTema['community'].isin(list(comunidades.values()))]
            listaComunidad.append(len(dfComunidad['author'].unique()) / sum(usuariosComunidades))

            # itera sobre cada comunidad
            for comunidad, usuarios in zip(list(comunidades.values()), usuariosComunidades):
                dfComunidad = dfTema[dfTema['community'] == comunidad]
                listaComunidad.append(len(dfComunidad['author'].unique()) / usuarios)
            
            listaTema.append(listaComunidad)

        df = pd.DataFrame(listaTema)
        # df.columns = df.iloc[0]
        # df = df[1:]
        df.columns = ['Temas','Total'] + list(comunidades.keys()) 
        return df

    def tematicasInteresPonderado(
        self, dataframe: pd.DataFrame, comunidades: dict
    ) -> pd.DataFrame:
        '''
        Porcentaje de activacion de cada comunidad, con respecto a cada tema, 
        compensando la difencia de cantidad de autores de cada comunidad.
        Debe tener como entrada el DF que sale de activacionComunidadTotal
        '''
        # Suma porcentajes de activacion de las comunidades, por tema
        dataframe['suma porcentajes'] = dataframe[list(comunidades.keys())].sum(axis=1)

        # Expresa el porcentaje de activacion de cada comunidad, como la participacion que tiene en la activacion total
        for comunidad in list(comunidades.keys()):
            dataframe[comunidad] = dataframe[comunidad] / dataframe['suma porcentajes']

        dataframe.drop('suma porcentajes', axis=1, inplace=True)
        dataframe.drop('Total', axis=1, inplace=True)
        return dataframe


    def histogramaMensajesSinFiltrar(self, dataframe: pd.DataFrame
    ) -> pd.DataFrame:
        ''' Histograma semanal de todos los mensajes pertenezcan o no a algun tema
        '''
        sf = dataframe.groupby(['semana'])['semana'].count()
        return  pd.DataFrame({'semana':sf.index, 'mensajes':sf.values})


    def histogramaMensajesAgrupados(
        self, dataframe: pd.DataFrame,
        temasFavorables: list, temasDesfavorables: list
    ) -> pd.DataFrame:
        ''' Histograma semanal de todos los mensajes que pertenecen a temas
            favorables y desfavorables
        '''
        dataframe = self._reemplazarXpor1(dataframe)
        # Coloca 1 en la columna 'mensajes favorables' si el mensaje pertenece a alguno de esos temas
        dataframe['mensajes favorables'] = dataframe[temasFavorables].sum(axis=1)
        dataframe.loc[dataframe['mensajes favorables'] > 1, 'mensajes favorables'] = 1
         # Coloca 1 en la columna 'mensajes desfavorables' si el mensaje pertenece a alguno de esos temas
        dataframe['mensajes desfavorables'] = dataframe[temasDesfavorables].sum(axis=1)
        dataframe.loc[dataframe['mensajes desfavorables'] > 1, 'mensajes desfavorables'] = 1

        # suma la cantidad de mensajes de temas favorables y desfavorables
        dataframe = dataframe.groupby(['semana'])['mensajes favorables', 'mensajes desfavorables'].sum()
        '''
        # columna con total de mensajes por semana
        dataframe['total'] =  dataframe['mensajes favorables'] +  dataframe['mensajes desfavorables']
        # expresa cantidades de mensajes como porcentajes
        dataframe['mensajes favorables'] = dataframe['mensajes favorables'] / dataframe['total']
        dataframe['mensajes desfavorables'] = dataframe['mensajes desfavorables'] / dataframe['total']
        '''
        return dataframe


    def ventajaSemana(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        ''' Cantidad de semanas en que cada partido tuvo mayor cantidad
            de mensajes que el otro.
            Recibe DF del metodo self.histogramaMensajesAgrupados
        '''
        dataframe.loc[dataframe['mensajes favorables'] > 
                      dataframe['mensajes desfavorables'], 'progresistas'] = 1
        dataframe.loc[dataframe['mensajes favorables'] < 
                      dataframe['mensajes desfavorables'], 'conservadores'] = 1
        
        dataframe = dataframe[['progresistas','conservadores']].sum().to_frame()
        dataframe.columns = ['Semanas con ventaja']
        return dataframe


    def _reemplazarXpor1(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        '''
        Reemplaza 'X' por 1 y NaN por 0
        '''
        dataframe.replace('X', 1, inplace=True)
        dataframe.fillna(0, inplace=True)
        return dataframe

    def _contarAutoresComunidad(self, path:str) -> int:
        '''
        Cuenta cantidad de autores en un fichero especifico a una comundad
        '''
        #nombre_archivo = 'unique_authors_' + community + '.txt'
        count = 0
        with open(path, 'r') as file:
            palabras = file.read()
            count = len(palabras.split(", "))
        return count












