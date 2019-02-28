Outil d'analyse des résultats de la commande "indexStats" de mongo
==============================

Cet outil contient 2 scripts python permettant de faire :
- script "ANALYZER" : analyser le résultat de la commande ``indexStats`` de Mongo (dispo depuis la v3.2) sur un cluster de X noeuds
- script "AGGREGATOR" : aggréger un ensemble de résultats de cette analyse de manière synthétique (dans différents type de sortie)

Pré-requis
---
- python 2.7.X + installation des dépendances (`pip install -r requirements.txt`)
    - bonne pratique : travailler dans un environnement python isolé (`virtualenv`) : utiliser `workon`, `mkvirtualenv`, `lsvirtualenv`, ...
- 1 fichier de résultat brut par noeud du cluster mongo issu de la commande suivante :
    ``db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )``
    - ATTENTION : il se peut qu'il y ait plusieurs pages de résultats (ATTENTION au ``Type "it" for more``)
- procédure de lancement de la commande ``indexStats`` sur un noeud du cluster mongo
    - se connecter au mongo avec un utilisateur ayant les droits d'effectuer une opération d'administration
    - sur le noeud master
        - ``use MA_BASEDEDONNEES;``
        - ``db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )``
    - sur un esclave :
        - ``rs.slaveOk()``
        - ``use MA_BASEDEDONNEES;``
        - ``db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )``
        - ``rs.slaveOk(false)``
- fortement conseillé : lancé chaque script avec l'option ``--help`` pour bien comprendre ce qui est possible de faire

TODOs
---
- tech
    - modulariser les scripts => découpage en module (cf mongo-injector)
    - utiliser un fichier requirements (`pip install -r requirements.txt`)
    - python 3 avec des classes
- evol scripts :
    - trier index : par nom, pas volumétrie
    - possibilité plusieurs slaves (+3 maintenant)
    - rendu wiki : trie inverse suivant date du tir (les plus récent d'abord)
    - fusionner les 2 scripts
    - gestion des stats de volumétrie physique (autre infos données dans KATS)