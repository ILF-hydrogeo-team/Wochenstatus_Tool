#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import os
import numpy as np
import math
from datetime import datetime
import shutil
from openpyxl import load_workbook


# In[2]:


LogfileText = ""


# In[3]:
def check_for_file(file_name: str, file_path: str):
    '''

    :param file_name: Name of file
    :param file_path: Folder where file should be located
    :return:
    This function checks if the file exists or not. If not it returns an error. If it does, it returns the path
    of the file.
    '''

    full_path = f"{file_path}/{file_name}"
    try:
        if file_name in os.listdir(file_path):
            new_path = full_path
    except:
        LogfileText = LogfileText + f"\nKann den automatischen GeoDin Export nicht finden oder nicht darauf zugreifen unter\n {full_path} \n Verwende stattdessen Datei im selben Verzeichnis wie in diesem Skript.\n"
        try:
            if file_name in os.listdir("."):
                new_path = file_name
            else:
                LogfileText = LogfileText + f"\nKann {file_name} auch im selben Verzeichnis nicht finden (/nicht darauf zugreifen)\n"
        except:
            LogfileText = LogfileText + f"\nFehler im Abruf von {file_name}\n"
    return new_path

geodinExportFilePath = check_for_file(file_name = 'Suedlink_MWuB_GWAnalytik.csv',
                                      file_path = r"I:/ATIBK_Projects/R794/5_WS/58_GEOL/LA225_Monitoring_Phase_1_2/_WWBS_LA225/_mDB_VHT/Export_geodin-SQL/automatisch")

StammdatenFilePath = check_for_file(file_name = 'SQL-Abfrage_Suedlink_MWuB_GWStammdaten_mit OG_20251119_final.xlsx',
                                    file_path = r"I:/ATIBK_Projects/R794/5_WS/58_GEOL/LA602_Monitoring_Phase_2_2/_WWBS_LA602/_mDB_VHT/SQL-Abfragen/_Testing")

# In[4]:


try:

    ### IMPORT Aller Daten aus geodin für Statistik

    with open(geodinExportFilePath, 'r') as file:
        # Read all lines from the file
        lines = file.readlines()

        PRJ_ID = []
        LONGNAME = []
        SMPNAME = []
        SMPDATE = []
        SMPTIME = []
        ABSTICH = []
        TWA = []
        EC = []
        PH_FIELD = []
        O2 = []
        TURB = []
        Sonstiges = []



        # Process each line
        for line in lines:
            # Split the line into fields using semicolon as the separator
            fields = line.strip().split(';')
            PRJ_ID.append(fields[0])
            LONGNAME.append(fields[1])
            SMPNAME.append(fields[2])
            SMPDATE.append(fields[3])
            SMPTIME.append(fields[4])
            try:
                ABSTICH.append(float(fields[5]))
            except:
                ABSTICH.append(np.nan)
            try:
                TWA.append(float(fields[6]))
            except:
                TWA.append(np.nan)
            try:
                EC.append(float(fields[7].replace(",","")))
            except:
                EC.append(np.nan)
            try:
                PH_FIELD.append(float(fields[8]))
            except:
                PH_FIELD.append(np.nan)
            try:
                O2.append(float(fields[9]))
            except:
                O2.append(np.nan)
            TURB.append(fields[10])
            Sonstiges.append(fields[11:])
            if len(fields[11:])!= 4:
                print(fields[11:])
                LogfileText = LogfileText+"\nFehler in der Anzahl der Spalten. Überprüfe Eintrag zu ",fields[1]," am ",fields[3],fields[4],"\nEs dürfen keine \";\" im frei Text verwendet werden!!"

    gd_PRJ_ID = np.array(PRJ_ID)
    gd_LONGNAME = np.array(LONGNAME) #use this! not SMPNAME
    gd_SMPNAME = np.array(SMPNAME) #dont use this - teilweise AUH IDs
    gd_SMPDATE = np.array(SMPDATE)
    gd_SMPTIME = np.array(SMPTIME)
    gd_ABSTICH = np.array(ABSTICH)
    gd_TWA = np.array(TWA)
    gd_EC = np.array(EC)
    gd_PH_FIELD = np.array(PH_FIELD)
    gd_O2 = np.array(O2)
    gd_TURB = np.array(TURB)

    # create gd_ZEIT
    zeitli = []
    for i in range(0,len(gd_SMPDATE)):
        if len(gd_SMPTIME[i])>4:
            dt = datetime(year = int(gd_SMPDATE[i][6:10]), month = int(gd_SMPDATE[i][3:5]), day = int(gd_SMPDATE[i][0:2]), hour = int(gd_SMPTIME[i][0:2]), minute = int(gd_SMPTIME[i][3:5]))
        else:
            dt = datetime(year = int(gd_SMPDATE[i][6:10]), month = int(gd_SMPDATE[i][3:5]), day = int(gd_SMPDATE[i][0:2])) # TODO: Hier zufügen: Zeit!
        zeitli.append(dt)
    gd_ZEIT = np.array(zeitli)

    LogfileText = LogfileText+"\nAnzahl Zeilen im Geodin Export: "+str(len(gd_LONGNAME))+"\n"

    ####### make stammdaten into stammdaten_df

    stammdaten_df = pd.read_excel(StammdatenFilePath)


    ####### COPY xlsx aus der Vorlage

    bearbZeitStr = str(datetime.today().strftime("%Y%m%d-%H%M%S"))
    shutil.copyfile("Vorlage_Bericht_NICHT-VERAENDERN/Vorlage_NICHT-VERAENDERN.xlsx", "Suedlink_WochenStatus_"+bearbZeitStr+".xlsx")
    #paBF.to_excel("SkriptOutput_"+bearbZeitStr+".xlsx", index=False)


    ###### df für export

    Werteli = ['ab','wa','el','ph','o2']
    Parameterli = ['min','min20','max80','max','mittel','anzahl','letzter','aktuell']

    dfcolimp = ['querung', 'id','zeit-aktuell']
    for el1 in Werteli:
        for el2 in Parameterli:
            dfcolimp.append(el1+"-"+el2)
    dfcolimp.append('tr-letzter')
    dfcolimp.append('tr-aktuell')
    dfcolimp.append('zeit-ersterDP')
    dfcolimp.append('zeit-letzterDP')

    df = pd.DataFrame(columns=dfcolimp)


    ###### Import ILF-Wöchentlich

    if len(os.listdir("ImportSkript/woechentlich-eigene")) != 1:
        LogfileText = LogfileText+"\nEs liegt nicht genau 1 Datei in ImportSkript/woechentlich-eigene"

    LogfileText = LogfileText+"\nFür den wöchentlichen Bericht der ILF-Messstellen wurde die Datei \""+os.listdir("ImportSkript/woechentlich-eigene")[0]+"\" verwendet\n"
    egdf = pd.read_excel("ImportSkript/woechentlich-eigene/"+os.listdir("ImportSkript/woechentlich-eigene")[0])

    for line in egdf["INVID"]:
        if "_ds" not in line:
            #Zeit
            smpdate = str(egdf["SMPDATE"][np.where(egdf["INVID"]==line)[0][0]])
            smptime = str(egdf["SMPTIME"][np.where(egdf["INVID"]==line)[0][0]])
            if smptime=="nan":
                smptime = "00:00"
            dt = datetime(year = int(smpdate[0:4]), month = int(smpdate[5:7]), day = int(smpdate[8:10]), hour = int(smptime[0:2]), minute = int(smptime[3:5]))
            #5 Werte (Trübung fehlt noch)
            A = egdf["A"][np.where(egdf["INVID"]==line)[0][0]]
            WT = egdf["W_TEMP"][np.where(egdf["INVID"]==line)[0][0]]
            EL = egdf["LF"][np.where(egdf["INVID"]==line)[0][0]]
            PH = egdf["PHWERT"][np.where(egdf["INVID"]==line)[0][0]]
            O2 = egdf["O2_FELD"][np.where(egdf["INVID"]==line)[0][0]]

            newRow = {'id': [line], 'zeit-aktuell': [dt], 'ab-aktuell': [A], 'wa-aktuell': [WT], 'el-aktuell': [EL], 'ph-aktuell': [PH], 'o2-aktuell': [O2]}
            newRow = pd.DataFrame(newRow)
            df = pd.concat([df,newRow], ignore_index=True)

    ###### Import 3te-Wöchentlich

    if len(os.listdir("ImportSkript/woechentlich-dritte")) != 1:
        LogfileText = LogfileText+"\nEs liegt nicht genau 1 Datei in ImportSkript/woechentlich-dritte"

    LogfileText = LogfileText+"\nFür den wöchentlichen Bericht der Messstellen Dritter wurde die Datei \""+os.listdir("ImportSkript/woechentlich-dritte")[0]+"\" verwendet\n"
    drdf = pd.read_excel("ImportSkript/woechentlich-dritte/"+os.listdir("ImportSkript/woechentlich-dritte")[0])

    for line in drdf["INVID"]:
        if "PA" in line:
            #Zeit
            smpdate = str(drdf["SMPDATE"][np.where(drdf["INVID"]==line)[0][0]])
            dt = datetime(year = int(smpdate[0:4]), month = int(smpdate[5:7]), day = int(smpdate[8:10]), hour = int(smpdate[11:13]), minute = int(smpdate[14:16]))
            #5 Werte (Trübung fehlt noch)
            A = drdf["WLV_COLLAR [m]"][np.where(drdf["INVID"]==line)[0][0]]
            WT = drdf["WAT [°C]"][np.where(drdf["INVID"]==line)[0][0]]
            EL = drdf["ELL [µS/cm]"][np.where(drdf["INVID"]==line)[0][0]]
            PH = drdf["PH [–]"][np.where(drdf["INVID"]==line)[0][0]]
            O2 = drdf["O2 [mg/l]"][np.where(drdf["INVID"]==line)[0][0]]

            newRow = {'id': [line], 'zeit-aktuell': [dt], 'ab-aktuell': [A], 'wa-aktuell': [WT], 'el-aktuell': [EL], 'ph-aktuell': [PH], 'o2-aktuell': [O2]}
            newRow = pd.DataFrame(newRow)
            df = pd.concat([df,newRow], ignore_index=True)


    ###### Füge die Daten der Statistik hinzu (aus dem automatischen geodin Export)

    ArrayWerteLi = [gd_ABSTICH, gd_TWA, gd_EC, gd_PH_FIELD, gd_O2] #MUSS MIT REIHNFOLGE von Werteli (oben) übereinstimmen!!
    #Werteli = ['ab','wa','el','ph','o2']
    #Parameterli = ['min','min20','max80','max','mittel','anzahl','letzter','aktuell']

    # QuerungsID einfüllen
    mapping = stammdaten_df.set_index('LONGNAME')['BauwerksID']
    df['querung'] = df['id'].map(mapping)

    for ID in df["id"]:

        # Kontrolliere ob überall Vorwerte
        mask = (gd_LONGNAME == ID)
        matching_dates = gd_ZEIT[mask]    
        # Check if there are any matching dates
        if len(matching_dates) == 0:
            print(f"⚠️ Keine Vorwerte für {ID}, nur die aktuelle Werte werden benützt...")
            # Leave the fields empty (they will stay NaN in df)
            continue
        
        df.loc[df["id"]==ID,"zeit-ersterDP"] = np.min(gd_ZEIT[np.where(gd_LONGNAME==ID)])
        df.loc[df["id"]==ID,"zeit-letzterDP"] = np.max(gd_ZEIT[np.where(gd_LONGNAME==ID)])
        
        for i in range(0,len(ArrayWerteLi)):
            arr = ArrayWerteLi[i]
            WerteMitNan = arr[np.where(gd_LONGNAME==ID)]
            Werte = WerteMitNan[np.where(np.isnan(WerteMitNan)==False)]
            WerteZeit = gd_ZEIT[np.where(gd_LONGNAME==ID)]
            WerteZeit = WerteZeit[np.where(np.isnan(WerteMitNan)==False)]

            if len(Werte)>0:
                df.loc[df["id"]==ID, Werteli[i]+"-min"] = np.min(Werte)
                df.loc[df["id"]==ID, Werteli[i]+"-max"] = np.max(Werte)
                df.loc[df["id"]==ID, Werteli[i]+"-mittel"] = np.round(np.average(Werte),2)
                n = len(Werte)
                df.loc[df["id"]==ID, Werteli[i]+"-anzahl"] = n
                letzter = Werte[np.where(WerteZeit==np.max(WerteZeit))]
                if len(letzter)==1:
                    df.loc[df["id"]==ID, Werteli[i]+"-letzter"] = letzter[0]
                else:
                    LogfileText = LogfileText+"\nFür "+ID+" gibt es zum Parameter "+Werteli[i]+" mehrere 'letzte' Werte (gleiches Datum)\n"

                RemoveDP = int(n/5)
                if RemoveDP > 0:
                    WerteSortiert = np.sort(Werte)
                    df.loc[df["id"]==ID, Werteli[i]+"-min20"] = np.min(WerteSortiert[RemoveDP:])
                    df.loc[df["id"]==ID, Werteli[i]+"-max80"] = np.max(WerteSortiert[:-RemoveDP])
                else:
                    df.loc[df["id"]==ID, Werteli[i]+"-min20"] = np.min(Werte)
                    df.loc[df["id"]==ID, Werteli[i]+"-max80"] = np.max(Werte)
            else:
                df.loc[df["id"]==ID, Werteli[i]+"-anzahl"] = 0                
    
    print("\n Dein Excelfile mit Auswertungen ist fertig!")
    df.to_excel('SkriptOutput_'+bearbZeitStr+'.xlsx', index=False)


except:
    LogfileText = LogfileText+"\nFehler im Ablauf des Skriptes. Vorgang nicht beendet.\n"
    


# In[5]:


LogfileText = LogfileText+"\nSkript finished: "+bearbZeitStr
with open("LogFile.txt", "w") as file:
    file.write(LogfileText)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




