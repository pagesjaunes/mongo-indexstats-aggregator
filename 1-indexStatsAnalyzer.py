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

# constantes
CONST_NOEUD_PRIM = "PRIMARY"
CONST_NOEUD_SEC1 = "SECONDARY-1"
CONST_NOEUD_SEC2 = "SECONDARY-2"

CONST_CHAMPS_NAME = "name"
CONST_CHAMPS_HOST = "host"
CONST_CHAMPS_ACCESSES = "accesses"
CONST_CHAMPS_ACCESSES_OPS = "ops"
CONST_CHAMPS_ACCESSES_SINCE = "since"
CONST_CHAMPS_NB = "nb"
CONST_CHAMPS_DATE_DEBUT = "date_debut"
CONST_CHAMPS_NB_GLOBAL = "nb_global"

#
# Remplacement des format Long et Date de Mongo par leur équivalent en json
#
def replaceLongAndDate(line):
    #log_trace("line BEFORE_REPLACE= " + line)

    # remplacement des NumberLong
    line = re.sub(r'NumberLong\(([0-9]*)\)',
                    r'{"$numberLong": "\1"}',
                    line)

    # remplacement des ISODate
    line = re.sub(r'ISODate\((\S*)\)',
                    r'{"$date": \1}',
                    line)

    #log_trace("line AFTER_REPLACE= " + line)
    return line

#
# Récupération du fichier d'un certain type dans un répertoire
#
def getJsonFile(fileType, dirName):
    fprimTab = glob.glob(dirName + '/*'+fileType+'.json.orig')

    utils.log_trace("fprimTab = " + str(len(fprimTab)))
    if len(fprimTab) > 1 :
        utils.log_erreur('Plusieurs fichiers "'+fileType+'" present dans le repertoire "' + dirName + '"\n')
    if len(fprimTab) == 0 :
        utils.log_erreur('Pas de fichier "'+fileType+'" present dans le repertoire "' + dirName + '"\n')
    
    utils.log_debug('Fichier "'+fileType+'" trouve = ' + fprimTab[0])

    try:
        fic = open(fprimTab[0], "r")
    except:
        utils.log_erreur("Erreur durant l'ouverture du fichier " + fprimTab[0])
    
    return fic

#
# Récupération de la liste des fichiers d'un répertoire tout en validant le paramètre "dirName"
#
def ETAPE1_validateDirParamAndGetFilesTab(dirName, paramName):
    utils.log_debug('Controle parametre obligatoire "'+paramName+'"')
    if dirName is None:
        utils.log_erreur(paramName+' est obligatoire\n')

    utils.log_debug('Controle validite du repertoire "'+dirName+'"')
    if not os.path.isdir(dirName):
        utils.log_erreur(dirName+' n\'est pas un repertoire\n')

    utils.log_debug('Recuperation du fichier de type "'+CONST_NOEUD_PRIM+'" dans le repertoire "'+dirName+'"')
    fprim = getJsonFile(CONST_NOEUD_PRIM, dirName)
    
    utils.log_debug('Recuperation du fichier de type "'+CONST_NOEUD_SEC1+'" dans le repertoire "'+dirName+'"')
    fsec1 = getJsonFile(CONST_NOEUD_SEC1, dirName)
    
    utils.log_debug('Recuperation du fichier de type "'+CONST_NOEUD_SEC2+'" dans le repertoire "'+dirName+'"')
    fsec2 = getJsonFile(CONST_NOEUD_SEC2, dirName)

    ficTab = [fprim, fsec1, fsec2]

    return ficTab

#
# Récupération des données d'un fichier
#
def ETAPE2_remplirMapIndexAvecFichier(file, type_noeud):
    utils.log_debug('Remplissage de la MAP pour le noeud "'+type_noeud+'"')

    for line in file : 

        # retravaille de la ligne et parsing JSON
        line = replaceLongAndDate(line)
        data = json_util.loads(line)
        
        index_name = data[CONST_CHAMPS_NAME]
        host = data[CONST_CHAMPS_HOST]
        nb = data[CONST_CHAMPS_ACCESSES][CONST_CHAMPS_ACCESSES_OPS]
        date_deb = data[CONST_CHAMPS_ACCESSES][CONST_CHAMPS_ACCESSES_SINCE]

        mapHostInfo = {CONST_CHAMPS_HOST : host, CONST_CHAMPS_NB : nb, CONST_CHAMPS_DATE_DEBUT : date_deb}

        if type_noeud == CONST_NOEUD_PRIM:
            utils.log_trace("Initialisation de l'index '" + index_name + "' pour le noeud " + type_noeud)
            mapIndex[index_name] = {CONST_CHAMPS_NB_GLOBAL : nb , CONST_NOEUD_PRIM : mapHostInfo, CONST_NOEUD_SEC1 : {}, CONST_NOEUD_SEC2 : {}}
        else:
            if index_name in mapIndex :
                utils.log_trace("Update de l\'index '" + index_name + "' pour le noeud " + type_noeud)
                mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL] = mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL] + nb
                mapIndex[index_name][type_noeud] = mapHostInfo
            else:
                utils.log_erreur("L'index courant '" + index_name + "' n'existe pas encore dans le fichier du noeud " + CONST_NOEUD_PRIM + " => pas normal !")

#
# Affichage des données aggrégées d'un noeud mongo
#
def afficherDonneesDuNoeud(index_name, noeud, line_prettyprint):
    utils.log_debug('Affichage des donnees de l\'index "'+index_name+'" pour le noeud "'+noeud+'"')

    if mapIndex[index_name][noeud] :
        host = mapIndex[index_name][noeud][CONST_CHAMPS_HOST]
        nb = mapIndex[index_name][noeud][CONST_CHAMPS_NB]
        nb_humanize = utils.humanize_int(nb)
        date_deb = str(mapIndex[index_name][noeud][CONST_CHAMPS_DATE_DEBUT])
        if args.prettyprint:
            line_prettyprint = line_prettyprint + "||| " + nb_humanize + " "
        elif args.miniprint:
            line_prettyprint = line_prettyprint + "|||" + str(nb)
        else:
            utils.log_retourchariot("   "+noeud+" = " + host)
            utils.log_retourchariot("      "+CONST_CHAMPS_NB+" = " + nb_humanize)
            utils.log_retourchariot("      "+CONST_CHAMPS_DATE_DEBUT+" = " + date_deb)
    else:
        utils.log_erreur("L'index courant '" + index_name + "' n'existe pas pour le noeud " + CONST_NOEUD_PRIM + " => pas normal !")

    return line_prettyprint

#
# Méthode utilitaire d'affichage colorisée dans la console (si option active)
#
def calculerChaineAAfficher(chaine, type):
    if args.color:
        if type=="ERREUR":
            return "\033[37;41;5m "+chaine+" \033[0m"
        if type=="WARN":
            return "\033[43;1m "+chaine+" \033[0m"
        if type=="INFO":
            return "\033[46;1m "+chaine+" \033[0m"
    return chaine

#
# Affichage des données aggrégées des tous les fichiers
#
def ETAPE3_afficherDonnees():
    utils.log_debug('Affichage des donnees')
    nbIndex=0
    nbIndexNonUtilises=0
    nbIndexPeuUtilises=0
    nbIndexTresPeuUtilises=0
    nbIndexTresUtilises=0
    for key in sorted(mapIndex.keys()) :
        index_name = key
        nb_global = mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL]
        line_prettyprint = ""

        nb_global_str_human = utils.humanize_int(nb_global)
        if nb_global==0:
            # log("0 => "+str(len(nb_global)))
            nb_global_str_human = calculerChaineAAfficher(nb_global_str_human, "ERREUR")
            nbIndexNonUtilises = nbIndexNonUtilises + 1
        elif nb_global < 10 :
            # log("<2 => "+str(len(nb_global)))
            nb_global_str_human = calculerChaineAAfficher(nb_global_str_human, "WARN")
            nbIndexTresPeuUtilises = nbIndexTresPeuUtilises + 1
        elif nb_global < 100 :
            # log("<3 => "+str(len(nb_global)))
            nb_global_str_human = calculerChaineAAfficher(nb_global_str_human, "WARN")
            nbIndexPeuUtilises = nbIndexPeuUtilises + 1

        if nb_global > 100000 :
            # log(">5 => "+str(len(nb_global)))
            nb_global_str_human = calculerChaineAAfficher(nb_global_str_human, "INFO")
            nbIndexTresUtilises = nbIndexTresUtilises + 1

        if args.prettyprint:
            line_prettyprint = index_name + " "
        elif args.miniprint:
            line_prettyprint = index_name
        else:
            utils.log_retourchariot(CONST_CHAMPS_NAME + " = " + index_name)
            utils.log_retourchariot("   "+CONST_CHAMPS_NB_GLOBAL+" = " + nb_global_str_human)

        line_prettyprint = afficherDonneesDuNoeud(index_name, CONST_NOEUD_PRIM, line_prettyprint)
        line_prettyprint = afficherDonneesDuNoeud(index_name, CONST_NOEUD_SEC1, line_prettyprint)
        line_prettyprint = afficherDonneesDuNoeud(index_name, CONST_NOEUD_SEC2, line_prettyprint)

        if args.prettyprint:
            utils.log_retourchariot(line_prettyprint + "||| " + nb_global_str_human)
        elif args.miniprint:
            utils.log_retourchariot(line_prettyprint + "|||" + str(nb_global))
        else:
            utils.log_retourchariot("")

        nbIndex = nbIndex + 1
        
    if not args.miniprint :
        utils.log_retourchariot("")
        utils.log_retourchariot("NB indexs analyses : " + str(nbIndex))
        utils.log_retourchariot("NB indexs TRES utilises (>= 100 000) : "+calculerChaineAAfficher(str(nbIndexTresUtilises), "INFO"))
        utils.log_retourchariot("NB indexs PEU utilises (< 100) : "+calculerChaineAAfficher(str(nbIndexPeuUtilises), "WARN"))
        utils.log_retourchariot("NB indexs TRES PEU utilises (< 10) : "+calculerChaineAAfficher(str(nbIndexTresPeuUtilises), "WARN"))
        utils.log_retourchariot("NB indexs NON utilises (= 0) : "+calculerChaineAAfficher(str(nbIndexNonUtilises), "ERREUR")+"\n")
    else :
        utils.log_retourchariot("%%%RESUME%%%nb_indexs|||" + str(nbIndex))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_tres_utilises_GE100000|||"+str(nbIndexTresUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_peu_utilises_LT100|||"+str(nbIndexPeuUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_tres_peu_utilises_LT10|||"+str(nbIndexTresPeuUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_non_utilises|||"+str(nbIndexNonUtilises))


def main(): 

    # Initialisation arguments
    global args
    parser = argparse.ArgumentParser(description='Script Python (2.7.X) permettant d\'aggreger les donnees \'indexStats\' de tous les noeuds d\'un cluster mongo (3 noeuds = 1 PRIMARY et 2 SECONDARY)')
    parser.add_argument('-d', '--dir', help='Repertoire contenant le resultat de la commande \'indexStats\' pour les 3 noeuds ('+CONST_NOEUD_PRIM+', '+CONST_NOEUD_SEC1+', '+CONST_NOEUD_SEC2+') du mongo')
    parser.add_argument('-p', '--prettyprint', help='Afficher des resultats de maniere minifiee', action='store_true')
    parser.add_argument('-m', '--miniprint', help='Afficher des resultats de maniere la plus minimaliste possible (pour futur traitement automatique du résultat)', action='store_true')
    parser.add_argument('-c', '--color', help='Afficher des resultats avec de la couleur', action='store_true')
    parser.add_argument('--debug', help='Afficher log de debug dans la console', action='store_true')
    parser.add_argument('--trace', help='Afficher log de debug et trace dans la console', action='store_true')
    
    args = parser.parse_args()

    # init des Level de debugging
    utils.set_trace_level(args.trace)
    utils.set_debug_level(args.debug | args.trace)

    # Controle options obligatoires
    filesTab = ETAPE1_validateDirParamAndGetFilesTab(args.dir, "--dir, -d")
    fprim = filesTab[0]
    fsec1 = filesTab[1]
    fsec2 = filesTab[2]

    # analyse des fichiers et mis de cote des donnees (dans une map)
    ETAPE2_remplirMapIndexAvecFichier(fprim, CONST_NOEUD_PRIM)
    fprim.close()

    ETAPE2_remplirMapIndexAvecFichier(fsec1, CONST_NOEUD_SEC1)
    fsec1.close()

    ETAPE2_remplirMapIndexAvecFichier(fsec2, CONST_NOEUD_SEC2)
    fsec2.close()

    # affichage des donnees consolidees
    if not args.miniprint:
        utils.log_retourchariot("")

    if args.miniprint:
        utils.log_retourchariot("# <INDEX_NAME>|||<NB_PRIMARY>|||<NB_SECONDARY1>|||<NB_SECONDARY2>|||<NB_GLOBAL>")
    elif args.prettyprint:
        utils.log_retourchariot("Format d'affichage : <INDEX_NAME> ||| <NB_PRIMARY> ||| <NB_SECONDARY1> ||| <NB_SECONDARY2> ||| <NB_GLOBAL>")
        utils.log_retourchariot("")
    

    ETAPE3_afficherDonnees()
    
if __name__ == '__main__':
    main()
