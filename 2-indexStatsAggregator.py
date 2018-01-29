# -*-coding:UTF-8 -*

import argparse
import os
import glob
import re
import json
from pprint import pprint

from bson import json_util

import utils

# conf pour session cassandra
args = None

# map des index et des leurs donnees
mapIndex = {}

# map des resumes et de meurs donnees
mapResume = {}

CONST_CHAMPS_RESUME_DATE_TIR = "%%%RESUME%%%date_tir"
CONST_CHAMPS_RESUME_DUREE = "%%%RESUME%%%duree"

#
# Validation du paramètre "output"
#
def validateOutParam(outputFormat, paramName):
    utils.log_debug('Controle parametre obligatoire "'+paramName+'"')
    if outputFormat is not None :
        if outputFormat != "md" and outputFormat != "wiki" :
            utils.log_erreur(paramName+' NON reconnue (cf help)\n')

#
# Récupération de la liste des fichiers à aggréger en validantle paramètre "dirName"
#
def ETAPE1_validateDirParamAndGetListResFiles(dirName, paramName):
    utils.log_debug('Controle parametre obligatoire "'+paramName+'"')
    if dirName is None:
        utils.log_erreur(paramName+' est obligatoire\n')

    utils.log_debug('Controle validite du repertoire "'+dirName+'"')
    if not os.path.isdir(dirName):
        utils.log_erreur(dirName+' n\'est pas un repertoire\n')

    utils.log_debug('Recuperation de la liste des fichiers de type "*.res" dans le repertoire "'+dirName+'"')
    ficTab = glob.glob(dirName + '/*.*')

    return ficTab

#
# Récupération des données des fichiers avec filtrage des lignes à analysées
#
def ETAPE2_remplirMapIndexAvecListeFichiers(ficNameTab):
    utils.log_debug('Remplissage de la MAP avec le(s) ' + str(len(ficNameTab)) + ' fichier(s) trouvés')

    for ficName in ficNameTab : 
        utils.log_debug("Ouverture fichier ["+ficName+"]")

        try:
            with open(ficName) as file:
                for line in file:
                    if line.startswith("#") :
                        utils.log_trace('COMMENTAIRE => ' + line, False)
                        continue
                    
                    if line.startswith("%%%RESUME%%%") :
                        # utils.log_trace('RESUME => '+line, False)
                        ajouterLigneIndexDansMapResume(line, ficName)
                        continue
                    
                    # utils.log_trace('Ligne INDEX => '+line)
                    ajouterLigneIndexDansMapIndex(line, ficName)

        except IOError as e:
            utils.log_erreur("Erreur durant l'ouverture du fichier [{0}] => I/O error({1}): {2}".format(ficName, e.errno, e.strerror))


def getInfosDuTir(ficNameAbsolute) :
    ficName = os.path.basename(ficNameAbsolute)
    ficNameWithoutExt = ficName.replace(".res", "")

    ficNameSplitTab = ficNameWithoutExt.split("_")
    if len(ficNameSplitTab) != 2 :
        raise Exception("Pb dans le formatage du nom du fichier => formatage accepté '<CHAINE>_<CHAINE FORMAT DATE YYYYMMDDHHmmSS>.res'".format(line))
    
    nomDuTir = ficNameSplitTab[0]
    dateDuTir = ficNameSplitTab[1]

    return nomDuTir, dateDuTir

#
# Analyse et mise de côté des données d'un ligne d'un fichier
#
def ajouterLigneIndexDansMapResume(line, ficNameAbsolute):
    utils.log_trace('RESUME => ' + line, False)

    lineTab = line.split("|||")
    if len(lineTab) != 2 :
        raise Exception("Pb dans le formatage de la ligne du résumé => il est nécessaire d'y avoir 2 chaines séparées par '|||' => ligne : {0}".format(line))
    
    attributeName = lineTab[0]
    value = lineTab[1]

    value = value.strip("\n")

    nomDuTir, dateDuTir = getInfosDuTir(ficNameAbsolute)

    # utils.log_trace('   index : "'+indexName+'"')
    # utils.log_trace("   ficName : {0}".format(ficName))
    # utils.log_trace("      ficNameWithoutExt : {0}".format(ficNameWithoutExt))
    # utils.log_trace("         nomDuTir : {0}".format(nomDuTir))
    # utils.log_trace("         dateDuTir : {0}".format(dateDuTir))
    # utils.log_trace("   index : {0}".format(indexName))
    # utils.log_trace("   nbGlobal : {0}".format(nbGlobal))

    if nomDuTir not in mapResume :
        dictResume = {attributeName : value}
        dictTir = {nomDuTir : dictResume}
        utils.log_trace("Tir '{0}' n'existe pas encore => AJOUT des données : {1}".format(nomDuTir, dictTir));
        mapResume.update(dictTir);
    else :
        dictTir = mapResume[nomDuTir]
        utils.log_trace("Tir '{0}' existe déjà => recup des données pour UPDATE : {1}".format(nomDuTir, dictTir));
        if attributeName not in dictTir :
            dictResume = {attributeName : value}
            utils.log_trace("Tir '{0}' n'existe pas encore => UPDATE des données : {1}".format(nomDuTir, dictTir));
            # utils.log_trace("AVANT MAJ : existe pas")
            dictTir.update(dictResume);
            # utils.log_trace("APRES MAJ : {0}".format(mapIndex[indexName][nomDuTir]))            
        else :
            dictResume = dictTir[attributeName]
            utils.log_trace(" +++++ ANOMALIE +++++ MotClé '{0}' existe déjà => PAS de UPDATE : PAS NORMAL ! => {1}".format(attributeName, dictResume));

#
# Analyse et mise de côté des données d'un ligne d'un fichier
#
def ajouterLigneIndexDansMapIndex(line, ficNameAbsolute):
    utils.log_trace('INDEX => ' + line, False)

    lineTab = line.split("|||")
    if len(lineTab) != 5 :
        raise Exception("Pb dans le formatage de la ligne d'index => il est nécessaire d'y avoir 5 chaines séparées par '|||' => ligne : {0}".format(line))
    
    indexName = lineTab[0]
    nbGlobal = lineTab[4]

    # contient le dernier carac de la ligne (retour chariot)
    nbGlobal = nbGlobal.strip("\n")

    nomDuTir, dateDuTir = getInfosDuTir(ficNameAbsolute)

    # utils.log_trace('   index : "'+indexName+'"')
    # utils.log_trace("   ficName : {0}".format(ficName))
    # utils.log_trace("      ficNameWithoutExt : {0}".format(ficNameWithoutExt))
    utils.log_trace("   index : {0}".format(indexName))
    utils.log_trace("   nbGlobal : {0}".format(nbGlobal))
    utils.log_trace("     nomDuTir : {0}".format(nomDuTir))
    utils.log_trace("     dateDuTir : {0}".format(dateDuTir))

    if indexName not in mapIndex :
        dictTir = {nomDuTir : nbGlobal}
        dictIndex = {indexName : dictTir}
        utils.log_trace("Index '{0}' n'existe pas encore => AJOUT des données : {1}".format(indexName, dictIndex));
        mapIndex.update(dictIndex);
    else :
        dictIndex = mapIndex[indexName]
        utils.log_trace("Index '{0}' existe déjà => recup des données pour UPDATE : {1}".format(indexName, dictIndex));
        if nomDuTir not in dictIndex :
            dictTir = {nomDuTir : nbGlobal}
            utils.log_trace("Tir '{0}' n'existe pas encore => UPDATE des données : {1}".format(nomDuTir, dictTir));
            # utils.log_trace("AVANT MAJ : existe pas")
            dictIndex.update(dictTir);
            # utils.log_trace("APRES MAJ : {0}".format(mapIndex[indexName][nomDuTir]))            
        else :
            dictTir = dictIndex[nomDuTir]
            utils.log_trace(" +++++ ANOMALIE +++++ Tir '{0}' existe déjà => PAS de UPDATE : PAS NORMAL ! => {1}".format(nomDuTir, dictTir));

#
# Affichage des données au format de sortie voulu
#
def ETAPE3_afficherDonnees(outputFormat):
    utils.log_debug("Affichage des donnees au format {0}".format("CSV" if outputFormat is None else "HTML"))

    tirNames = {}

    # récupération des noms des tirs pour affichage de l'entete
    for index_name in sorted(mapIndex.keys()) :
        # index_name = key
        dictTir = mapIndex[index_name]

        utils.log_trace("{0}".format(index_name))

        for tir_name in sorted(dictTir.keys()) :
            nb_global = dictTir[tir_name]

            dictTirNames = {tir_name : ""}
            tirNames.update(dictTirNames)

            utils.log_trace("   {0} - {1}".format(tir_name, nb_global))
    
    lstTirNames = sorted(tirNames.keys())
    # utils.log_trace('Affichage des noms des tirs TRIES (sous forme de liste)')
    # print lstTirNames

    # Affichage de l'entete (init) - PARTIE 1/3
    if outputFormat is None :
        ligneout = "# NOM_INDEX"
    elif outputFormat == "md" :
        ligneout = "indexes \ tirs"
    elif outputFormat == "wiki" :
        ligneout = "|| "

    # Affichage des noms des tirs dans l'entete - PARTIE 2/3
    for tir_name in lstTirNames :
        tir_date = str(mapResume[tir_name][CONST_CHAMPS_RESUME_DATE_TIR])
        tir_duree = str(mapResume[tir_name][CONST_CHAMPS_RESUME_DUREE])
        
        if outputFormat is None :
            ligneout = ligneout + ";" + tir_name + " (" + tir_date + " /// " + tir_duree + ")"
        elif outputFormat == "md" :
            ligneout = ligneout + " | " + tir_name + "\n" + tir_date + "\n" + tir_duree
        elif outputFormat == "wiki" :
            ligneout = ligneout + "||" + "{color:#FE00ED}" + tir_name + "{color}\n{color:#2D47EF}" + tir_date + "\n" + tir_duree + "{color}"

    # Affichage de l'entete (finalisation) - PARTIE 3/3
    if outputFormat is None :
        utils.log_retourchariot(ligneout)
    elif outputFormat == "md" :
        ligneout = ligneout + "\n"
        ligneout = ligneout + "---"
        for tir_name in lstTirNames :
            ligneout = ligneout + " | --- "
        ligneout = ligneout + "\n"
    elif outputFormat == "wiki" :
        ligneout = ligneout + "||\n"

    # Affichage des données de chaque index
    for index_name in sorted(mapIndex.keys()) :
        # index_name = key
        dictTir = mapIndex[index_name]

        if outputFormat is None :
            ligneout = index_name
        elif outputFormat == "md" :
            ligneout = ligneout + index_name
        elif outputFormat == "wiki" :
            # remplacement des "_" pour ne pas avoir le style "italic" actif dans Confluence
            index_name = index_name.replace("_", "\\_")
            ligneout = ligneout + "|" + index_name

        for tir_name in lstTirNames :
            nb_global = "/"
            try:
                nb_global = dictTir[tir_name]
            except KeyError:
                utils.log_trace("   Pas de tir '{0}' trouvé pour l'index '{1}'".format(tir_name, index_name))

            if outputFormat is None :   
                ligneout = ligneout + ";" + nb_global
            elif outputFormat == "md" :    
                ligneout = ligneout + " | " + nb_global
            elif outputFormat == "wiki" :    
                ligneout = ligneout + "|" + nb_global

        if outputFormat is None :
            utils.log_retourchariot(ligneout)
        elif outputFormat == "md" :
            ligneout = ligneout + "\n"
        elif outputFormat == "wiki" :
            ligneout = ligneout + "|\n"
        
    if outputFormat == "md" or outputFormat == "wiki" :
        utils.log(ligneout)

def main(): 

    # Initialisation arguments
    global args
    parser = argparse.ArgumentParser(description='Script Python (2.7.X) permettant d\'aggreger les donnees \'indexStats\' de tous les noeuds d\'un cluster mongo (3 noeuds = 1 PRIMARY et 2 SECONDARY)')
    parser.add_argument('-d', '--dir', help='Repertoire contenant le resultat minimifié de la commande \'indexStats\' du mongo')
    parser.add_argument('-o', '--out', help='Type de sortie du résultat (par défaut : "csv" / autres possibles : "md" (MarkDown), "wiki" (WIKI Confluence - "Balise WIKI"))')
    parser.add_argument('--debug', help='Afficher log de debug dans la console', action='store_true')
    parser.add_argument('--trace', help='Afficher log de debug et trace dans la console', action='store_true')
    
    args = parser.parse_args()

    # validation param out
    validateOutParam(args.out, "--out, -o")

    # init des Level de debugging
    utils.set_trace_level(args.trace)
    utils.set_debug_level(args.debug | args.trace)

    # Controle options obligatoires + recup liste fichieirs
    ficTab = ETAPE1_validateDirParamAndGetListResFiles(args.dir, "--dir, -d")
    
    # lecture des fichiers et mise de côté des données
    ETAPE2_remplirMapIndexAvecListeFichiers(ficTab)

    # Affichage des données
    ETAPE3_afficherDonnees(args.out)
    
if __name__ == '__main__':
    main()
