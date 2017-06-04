Usage
-----
After cloning the repository, execute:
```shell
mvn clean install
```
from the root directory. This creates a uberjar with all the dependencies.

**To create a master data set, execute**
```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar generate input_folder file_size <name1,size1>,<name2, size2>...
```
For instance
```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar generate generate sentiance 15 locations,64,sensors,138,devices,24
```

**To enlarge an existing master data set, execute:**

```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar update input_folder <name1,size1>,<name2, size2>...
```

For instance
```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar update sentiance locations,12,sensors,23,devices,10
```

**To backup an existing master data set, execute:**
```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar backup input_folder backup_folder
```

For instance
```shell
java -jar target/master-dataset-1.0-SNAPSHOT.jar backup sentiance sentiance_backup
