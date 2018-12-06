
I. En mode ligne de commande

1. Installer Hbase en mode Standalone (version 1.4.7)
    ==> télécharger depuis hbase.apache.org
2. Démarrer le serveur Hbase 
 $HBASE_HOME/bin/start_hbase.sh
3. Se connecter à Hbase shell
4. lister les tables présentes


II. Dans IntelliJ : 

1. Comprendre la structure du projet
2. Modifier les valeurs de nom de table et de Famille de colonne
3. Lancer le projet dans sa version initiale pour créer la table
4. Dans le HBASE SHELL , 
    i. observer la table 
    ii. effectuer des put, (put 'T' ,'aa','F1','AA:bob')
    iii. des get (put 'T' ,'aa')
    iv. et des scan (scan 'T', {ROWPREFIXFILTER => 'a'})
5. Dans IntelliJ, compléter le code de la méthode addData  et l'appeler
     i. ajouter des données,
     ii. lire par rowkey
     iii. lire par Scan
     

III. Mode cluster AWS : 

1. faire en sorte que le fichier hbase-site.xml utilisé par le programme soit celui de AWS. deux options à réaliser :
    i. remplacer le hase-site à l'intérieur du projet par le bon contenu
    ii. passer en argument du programme le chemin vers le bon hbase-site pour qu'il soit chargé lors de l'exécution
2. packager l'application sous forme de jar avec maven
3. talécharger le jar sur le cluster ou sur la machine EC2
4. Exécuter ce jar pour qu'il crée la table Hbase et insère des données

     
IV. Mode avancé : insérer de très grandes quantités de lignes dans Hbase

1. utiliser un ThreadPoolExecutor (modifier la méthode 'insertionMassive')
2. attacher un Objet de type Runnable qui fait de l'insertion de données
3. Lancer cette exécution.
    
  
  
    
    