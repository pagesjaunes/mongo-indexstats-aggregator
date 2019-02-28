# -*-coding:UTF-8 -*

import argparse
import os
import glob

import re

from bson import json_util

import utils
import datetime

# arguments de la ligne de commande
args = None

# map des index et des leurs donnees
mapIndex = {}

# extension des fichiers représentant le résultat de la commande mongo "indexstats"
CONST_FILE_EXTENSION = ".indexstats"

# liste des champs présent dans la réponse JSON dans un fichier résultat de la commande mongo "indexstats"
CONST_CHAMPS_NAME = "name"
CONST_CHAMPS_HOST = "host"
CONST_CHAMPS_ACCESSES = "accesses"
CONST_CHAMPS_ACCESSES_OPS = "ops"
CONST_CHAMPS_ACCESSES_SINCE = "since"

# liste des champs utilisés dans la map contenant les données aggrégées
CONST_CHAMPS_NB = "nb"
CONST_CHAMPS_DATE_DEBUT = "date_debut"
CONST_CHAMPS_NB_GLOBAL = "nb_global"

#
# Calcul du nom du noeud à partir d'un objet fichier
#
def getNodeNameFromFile(file):
    filename = os.path.basename(file.name)
    node_name = filename
    if node_name.endswith(CONST_FILE_EXTENSION) :
        node_name = node_name.rsplit('.', 1)[0]
    return node_name

def getBaseDirName(dirName) :
    dirName = re.sub("^/|/$", "", dirName)
    baseDirName = os.path.basename(dirName)
    return baseDirName

#
# Récupération de la liste des fichiers INDEXSTATS d'un répertoire (tout en validant le paramètre "dirName")
#
def ETAPE1_validateDirParamAndGetFilesTab(dirName, paramName):
    utils.log_debug("==> ETAPE1_validateDirParamAndGetFilesTab")

    # contrôle paramètre obligatoire correspondant au répertoire dans lequel sont stockés les fichiers INDEXSTATS
    utils.log_debug("Contrôle paramètre obligatoire [" + paramName + "]")
    if dirName is None:
        utils.log_erreur("[" + paramName + "] est obligatoire")

    # contrôle validité du répertoire
    utils.log_debug("Contrôle validité du répertoire [" + dirName + "]")
    if not os.path.isdir(dirName):
        utils.log_erreur("[" + dirName + "] n'est pas un répertoire")

    # suppression du 1er et dernier slash pour que le basename puisse fonctionner correctement
    # dirName = re.sub("^/|/$", "", dirName)
    baseDirName = getBaseDirName(dirName)

    # contrôle pattern du nom du répertoire
    utils.log_debug("Contrôle pattern du nom du répertoire [" + baseDirName + "]")
    if not re.match(".*_[0-9]{14}$", baseDirName) :
        utils.log_erreur("Le nom du répertoire [" + baseDirName + "] ne respecte pas le pattern '<NOM_TIR>_<DATE_TIR_FORMAT_YYYYMMDDHHmmss>' (exemple : 'PROD-G_20180118135500')")

    # récupération des fichiers INDEXSTATS du répertoire passé en paramètre
    pattern_fic_extension = "*" + CONST_FILE_EXTENSION
    utils.log_debug("Récupération des fichiers [" + pattern_fic_extension + "] dans le répertoire [" + dirName + "]")
    filesTab = glob.glob(dirName + "/" + pattern_fic_extension)

    # contrôle du nb de ficheirs trouvés
    if len(filesTab) == 0 :
        utils.log_erreur("Pas de fichiers INDEXSTATS présents dans le répertoire [" + dirName + "]")
    utils.log_debug("nb fic INDEXSTATS trouvés = " + str(len(filesTab)))

    # ouverture et mise de côté des fichiers INDEXSTATS trouvés
    lstFicOpened = []
    for file in filesTab : 
        try:
            ficOpened = open(file, "r")
            lstFicOpened.append(ficOpened)
        except:
            utils.log_erreur("Erreur durant l'ouverture du fichier [" + file + "]")

    utils.log_debug("nb fic INDEXSTATS ouverts avec succés = " + str(len(lstFicOpened)))

    return lstFicOpened

#
# Récupération des données d'un fichier
#
def ETAPE2_remplirMapIndexAvecFichier(filesTab):
    
    utils.log_debug("==> ETAPE2_remplirMapIndexAvecFichier")

    # Parcours de l'ensemble des fichiers INDEXSTATS passés en paramètre
    for file in filesTab :

        filename = os.path.basename(file.name)
        node_name = getNodeNameFromFile(file)

        utils.log_debug("Remplissage de la MAP pour le fichier [" + filename + "] (node_name = [" + node_name + "])")

        # parcours de l'ensemble de ligne du ficheir INDEXSTAT courant
        for line in file : 

            # retravaille de la ligne (remplacement Long et Date) et parsing JSON
            line = utils.mongoReplaceLongAndDate(line)
            data = json_util.loads(line)
            
            index_name = data[CONST_CHAMPS_NAME]
            host = data[CONST_CHAMPS_HOST]
            nb = data[CONST_CHAMPS_ACCESSES][CONST_CHAMPS_ACCESSES_OPS]
            date_deb = data[CONST_CHAMPS_ACCESSES][CONST_CHAMPS_ACCESSES_SINCE]

            # Sauvegarde dans la MAP des données des infos nécessaires pour la suite du traitement
            mapHostInfo = {CONST_CHAMPS_HOST : host, CONST_CHAMPS_NB : nb, CONST_CHAMPS_DATE_DEBUT : date_deb}

            if index_name not in mapIndex :
                utils.log_trace("MAP INDEX - Initialisation de l'index [" + index_name + "]")
                mapIndex[index_name] = {CONST_CHAMPS_NB_GLOBAL : 0}

            utils.log_trace("MAP INDEX - Update de l'index [" + index_name + "]")
            mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL] = mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL] + nb
            mapIndex[index_name][node_name] = mapHostInfo

        # fermeture du fichier INDEXSTAT préalablement ouvert
        file.close()

#
# Affichage des données d'entête
#
def ETAPE3_afficherDonneesEntete(filesTab) :
    
    utils.log_debug("==> ETAPE3_afficherDonneesEntete")
    
    if not args.miniprint:
        utils.log_retourchariot("")

    entete = ""
    separateur = ""
    if args.miniprint:
        entete = entete + "# "
    elif args.prettyprint:
        entete = entete + "Format d'affichage : "
        separateur = " "

    entete = entete + "<INDEX_NAME>"
    for file in filesTab :
        node_name = getNodeNameFromFile(file)
        entete = entete + separateur + "|||" + separateur + "<NB_" + node_name + ">"
    entete = entete + separateur  + "|||" + separateur + "<NB_GLOBAL>"

    utils.log_retourchariot(entete)

    # if args.prettyprint:
    if not args.miniprint:
        utils.log_retourchariot("")

#
# Affichage des données aggrégées d'un noeud mongo
#
def recupererDonneesDuNoeud(index_name, noeud, line_prettyprint):
    utils.log_debug("Récupération des données de l'index ["+ index_name + "] pour le noeud [" + noeud + "]")

    if mapIndex[index_name][noeud] :
        host = mapIndex[index_name][noeud][CONST_CHAMPS_HOST]
        nb = mapIndex[index_name][noeud][CONST_CHAMPS_NB]
        date_deb = str(mapIndex[index_name][noeud][CONST_CHAMPS_DATE_DEBUT])

        nb_humanize = utils.humanize_int(nb)
        nb_str = str(nb)

        if args.prettyprint:
            # TODO DDC : manque pas un espace séparteur avant les ||| ?
            line_prettyprint = line_prettyprint + "||| " + nb_humanize + " "
        elif args.miniprint:
            line_prettyprint = line_prettyprint + "|||" + nb_str
        else:
            utils.log_retourchariot("   " + noeud + " = " + host)
            utils.log_retourchariot("      " + CONST_CHAMPS_NB + " = " + nb_humanize)
            utils.log_retourchariot("      " + CONST_CHAMPS_DATE_DEBUT + " = " + date_deb)
    else:
        utils.log_erreur("L'index [" + index_name + "] n'existe pas pour le noeud [" + noeud + "] => pas normal ! (ANOMALIE TECH ?)")

    return line_prettyprint

#
# Méthode utilitaire d'affichage colorisée dans la console (si option active)
#
def coloriserChaineAAfficher(chaine, type):
    if args.color:
        if type=="ERREUR":
            return "\033[37;41;5m "+chaine+" \033[0m"
        if type=="WARN":
            return "\033[43;1m "+chaine+" \033[0m"
        if type=="INFO":
            return "\033[46;1m "+chaine+" \033[0m"
    return chaine

def getDateTir(dirName) :

    dateTir = None
    baseDirName = getBaseDirName(dirName)

    tab = baseDirName.split("_")
    dirDate = tab[len(tab)-1]
    utils.log_debug("tab.last="+dirDate)

    dateTir = datetime.datetime.strptime(dirDate, "%Y%m%d%H%M%S")

    return dateTir

def getDureeStats(dateTir, dateMin) :
    diff = dateTir - dateMin
    diffDays = diff.days
    diffSec = diff.seconds

    diffHeures = diffSec / 3600
    diffMinutes = diffSec % 3600 / 60

    dureeJrs = ""
    if diffDays > 0 :
        dureeJrs = str(diffDays) + " jrs "

    return dureeJrs + str(diffHeures) + "h" + str(diffMinutes)

#
# Affichage des données aggrégées des tous les fichiers
#
def ETAPE4_afficherDonnees(filesTab, dirName):
    
    utils.log_debug("==> ETAPE4_afficherDonnees")

    nbIndex=0
    nbIndexNonUtilises=0
    nbIndexPeuUtilises=0
    nbIndexTresPeuUtilises=0
    nbIndexTresUtilises=0
    dateMin=None
    dateMax=None
    dateTir=getDateTir(dirName)

    # parcours des index triés par nom
    for key in sorted(mapIndex.keys()) :
        #index_name = key
        index_name = str(key) # forçage en chaine car pb dans les logs si utilisation de l'accentuation avec index_name
        nb_global = mapIndex[index_name][CONST_CHAMPS_NB_GLOBAL]
        line_prettyprint = ""

        nb_global_str_human = utils.humanize_int(nb_global)

        # gestion de la colorisation et du dénombrement des indexs suivant les seuils d'utilisation
        if nb_global==0:
            # log("0 => "+str(len(nb_global)))
            nb_global_str_human = coloriserChaineAAfficher(nb_global_str_human, "ERREUR")
            nbIndexNonUtilises = nbIndexNonUtilises + 1
        elif nb_global < 10 :
            # log("<2 => "+str(len(nb_global)))
            nb_global_str_human = coloriserChaineAAfficher(nb_global_str_human, "WARN")
            nbIndexTresPeuUtilises = nbIndexTresPeuUtilises + 1
        elif nb_global < 100 :
            # log("<3 => "+str(len(nb_global)))
            nb_global_str_human = coloriserChaineAAfficher(nb_global_str_human, "WARN")
            nbIndexPeuUtilises = nbIndexPeuUtilises + 1

        if nb_global > 100000 :
            # log(">5 => "+str(len(nb_global)))
            nb_global_str_human = coloriserChaineAAfficher(nb_global_str_human, "INFO")
            nbIndexTresUtilises = nbIndexTresUtilises + 1

        # gestion affichage suivant l'option d'affichage choisie
        if args.prettyprint:
            line_prettyprint = index_name + " "
        elif args.miniprint:
            line_prettyprint = index_name
        else:
            utils.log_retourchariot(CONST_CHAMPS_NAME + " = " + index_name)
            utils.log_retourchariot("   "+CONST_CHAMPS_NB_GLOBAL+" = " + nb_global_str_human)

        # parcours des noeuds (donc des fichiers) afin de récupérer l'affichage des données du noeud
        for file in filesTab :
            node_name = getNodeNameFromFile(file)
            line_prettyprint = recupererDonneesDuNoeud(index_name, node_name, line_prettyprint)
            date_deb = mapIndex[index_name][node_name][CONST_CHAMPS_DATE_DEBUT]
            date_deb = date_deb.replace(tzinfo=None)
            utils.log_debug("date_deb = [" + str(date_deb) + "]")
            utils.log_debug("date_deb(23) = [" + str(date_deb)[:23] + "]")
            
            if dateMin is None :
                dateMin = date_deb
            elif date_deb < dateMin :
                dateMin = date_deb

            if dateMax is None :
                dateMax = date_deb
            elif date_deb > dateMax :
                dateMax = date_deb

        # gestion affichage du résultats du noeud
        if args.prettyprint:
            utils.log_retourchariot(line_prettyprint + "||| " + nb_global_str_human)
        elif args.miniprint:
            utils.log_retourchariot(line_prettyprint + "|||" + str(nb_global))
        else:
            utils.log_retourchariot("")

        # incrément du nb de noeud total
        nbIndex = nbIndex + 1

    # calcul de la durée des stats sur les indexs (date MIN - date TIR)
    # duree = None
    duree = getDureeStats(dateTir, dateMin)
        
    # Affichage du dénombrement des indexs (par seuil) suivant le type d'affichage
    if not args.miniprint :
        utils.log_retourchariot("")
        utils.log_retourchariot("NB indexs analyses : " + str(nbIndex))
        utils.log_retourchariot("NB indexs TRES utilises (>= 100 000) : " + coloriserChaineAAfficher(str(nbIndexTresUtilises), "INFO"))
        utils.log_retourchariot("NB indexs PEU utilises (< 100) : " + coloriserChaineAAfficher(str(nbIndexPeuUtilises), "WARN"))
        utils.log_retourchariot("NB indexs TRES PEU utilises (< 10) : " + coloriserChaineAAfficher(str(nbIndexTresPeuUtilises), "WARN"))
        utils.log_retourchariot("NB indexs NON utilises (= 0) : " + coloriserChaineAAfficher(str(nbIndexNonUtilises), "ERREUR"))
        utils.log_retourchariot("Date indexs MIN : " + str(dateMin))
        utils.log_retourchariot("Date indexs MAX : " + str(dateMax))
        utils.log_retourchariot("Date Tir : " + str(dateTir))
        utils.log_retourchariot("Durée (TIR_MIN) : " + str(duree)+"\n")
    else :
        utils.log_retourchariot("%%%RESUME%%%nb_indexs|||" + str(nbIndex))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_tres_utilises_GE100000|||" + str(nbIndexTresUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_peu_utilises_LT100|||" + str(nbIndexPeuUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_tres_peu_utilises_LT10|||" + str(nbIndexTresPeuUtilises))
        utils.log_retourchariot("%%%RESUME%%%nb_indexs_non_utilises|||" + str(nbIndexNonUtilises))
        utils.log_retourchariot("%%%RESUME%%%date_indexs_min|||" + str(dateMin))
        utils.log_retourchariot("%%%RESUME%%%date_indexs_max|||" + str(dateMax))
        utils.log_retourchariot("%%%RESUME%%%date_tir|||" + str(dateTir))
        utils.log_retourchariot("%%%RESUME%%%duree|||" + str(duree))


def main(): 

    # Initialisation arguments
    global args
    parser = argparse.ArgumentParser(description="Script Python (2.7.X) permettant d'aggréger les données 'indexStats' de tous les noeuds d'un cluster mongo")
    parser.add_argument("-d", "--dir", help="Répertoire contenant le résultat de la commande mongo 'indexStats' de chaque noeud du mongo")
    parser.add_argument("-p", "--prettyprint", help="Afficher des résultats de manière minifiée", action="store_true")
    parser.add_argument("-m", "--miniprint", help="Afficher des résultats de manière la plus minimaliste possible (pour futur traitement automatique du résultat)", action="store_true")
    parser.add_argument("-c", "--color", help="Afficher des résultats avec de la couleur", action="store_true")
    parser.add_argument("--debug", help="Afficher log de debug dans la console", action="store_true")
    parser.add_argument("--trace", help="Afficher log de debug et trace dans la console", action="store_true")
    
    args = parser.parse_args()

    # init des Level de debugging
    utils.set_trace_level(args.trace)
    utils.set_debug_level(args.debug | args.trace)

    filesTab = ETAPE1_validateDirParamAndGetFilesTab(args.dir, "--dir, -d")

    ETAPE2_remplirMapIndexAvecFichier(filesTab)

    ETAPE3_afficherDonneesEntete(filesTab)

    ETAPE4_afficherDonnees(filesTab, args.dir)
    
if __name__ == "__main__":
    main()
