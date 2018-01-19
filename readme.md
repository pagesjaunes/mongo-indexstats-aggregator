Outil d'analyse des résultats de la commande "indexStats" de mongo
==============================

Cet outil est un script python permettant de :
- analyses des résultats de la commande "indexStats" de Mongo (dispo depuis la v3.2) sur un cluster de 3 noeuds
- ressortir l'analyse sous différent format : standard, prettyprint, avec de la couleur

Pré-requis
---
- python 2.7.X
- 3 fichiers de résultats (PRIMARY, SECONDARY 1 et SECONDARY 2) : 1 fichier de résultat brut par noeud du cluster mongo résultat de la requête mongo suivante
    `db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )``
    - ATTENTION : il se peut qu'il y ait plusieurs pages de résultats (ATTENTION au 'Type "it" for more')
- procédure de lancement de la commande "indexStats" sur un noeud du cluster mongo - Exemple de la BD "CONTRIBUTION" sur la collection "Contribution"
    - se connecter au mongo avec un utilisateur ayant les droits d'effectuer des opérations d'administration
    - sur le master
        - use MA_BASEDEDONNEES;
        - db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )
    - sur un esclave :
        - rs.slaveOk()
        - use MA_BASEDEDONNEES;
        - db.MA_COLLECTION.aggregate( [ { $indexStats: { } } ] )
        - rs.slaveOk(false)

Infos techniques
---

- option "--help" dispo
- option "--prettyprint" : affichage au format simplifiée
- option "--color" : affichage avec de la couleur dans la console Shell

TODOs
---
- EVOL : rendre le nb de noeuds paramétrable
- EVOL : rendre la gestion des seuils pour les couleurs paramétrable
- ANO : option -p et -m incompatible => -p prends le pas  alors qu ene devrait pas (humanize avec des espaces)
