Outil d'analyse des résultats de la commande "indexStats" de mongo
==============================

Cet outil contient 2 scripts python permettant de faire :
- "ANALYZER" : analyser le résultat de la commande "indexStats" de Mongo (dispo depuis la v3.2) sur un cluster de 3 noeuds (pour le moment)
- "AGGREGATOR" : aggréger un ensemble de résultats de cette analyse de manière synthétique (dans différents type de sortie)

Pré-requis
---
- python 2.7.X
- 3 fichiers de résultats (PRIMARY, SECONDARY 1 et SECONDARY 2) => 1 fichier de résultat brut par noeud du cluster mongo (3 noeuds obligatoires pour le moment) issu de la commande suivante :
    ``db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )``
    - ATTENTION : il se peut qu'il y ait plusieurs pages de résultats (ATTENTION au ``Type "it" for more``)
- procédure de lancement de la commande "indexStats" sur un noeud du cluster mongo
    - se connecter au mongo avec un utilisateur ayant les droits d'effectuer une opération d'administration
    - sur le noeud master
        - use MA_BASEDEDONNEES;
        - db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )
    - sur un esclave :
        - rs.slaveOk()
        - use MA_BASEDEDONNEES;
        - db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )
        - rs.slaveOk(false)

Infos techniques
---
- fortement conseillé : lancé chaque script avec l'option ``--help``pour bien comprendre ce qui est possible de faire

TODOs
---
- EVOL : rendre le nb de noeuds paramétrable ou suppimer la gestion des noeuds
- EVOL : rendre la gestion des seuils pour les couleurs paramétrable
- ANO : option -p et -m incompatible => -p prends le pas alors que ne devrait pas (humanize avec des espaces)
