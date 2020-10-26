import au.com.bytecode.opencsv.CSVReader;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.BsonDocument;
import org.bson.Document;
import java.io.FileReader;
import java.util.List;
import java.util.function.Consumer;

public class Main {

    public static void main(String[] args) throws Exception {

        MongoClient mongoClient = new MongoClient( "127.0.0.1" , 27017 );

        MongoDatabase database = mongoClient.getDatabase("local");

        // Создаем коллекцию
        MongoCollection<Document> collection = database.getCollection("Students");

        // Удалим из нее все документы
        collection.drop();

        //Получаем информацию о студентах их вложенного файла
        List<String[]> studentsInfo =  getStudentsInfoFromCSV(".\\src\\main\\resources\\mongo.csv");

        //Заполняем коллекцию данными о студентах
        for(String[] student : studentsInfo) {
            // Создадим первый документ
            Document firstDocument = new Document()
                    .append("name", student[0])
                    .append("age", student[1])
                    .append("courses", student[2]);

            // Вставляем документ в коллекцию
            collection.insertOne(firstDocument);
        }

        //Количество записей в коллекции
        System.out.println("Количество записей в коллекции " + collection.countDocuments());
        System.out.println();

        //Cтуденты старше 40
        System.out.println("Студенты старше 40:");
        BsonDocument query = BsonDocument.parse("{age: {$gt: '40'}}");
        collection.find(query).forEach((Consumer<Document>) System.out::println);
        System.out.println();

        //Самый молодой студент
        BsonDocument query1 = BsonDocument.parse("{age: 1}");
        String youngAge = collection.find().sort(query1).first().getString("age");
        BsonDocument query2 = BsonDocument.parse("{age: '" + youngAge + "'}");
        System.out.println("Один из самых молодых студентов" + collection.find(query2).first().getString("name"));
        System.out.println();

        BsonDocument query3 = BsonDocument.parse("{age: -1}");
        String oldAge = collection.find().sort(query1).first().getString("age");
        System.out.println("Список курсов одного из самых старых студентов: " + collection.find().sort(query3).first().getString("courses"));
        System.out.println();

        System.out.println("Список самых молодых студентов");
        BsonDocument query5 = BsonDocument.parse("{age: '" + youngAge + "'}");
        collection.find(query5).forEach((Consumer<Document>) document -> {
                    System.out.println(document.getString("name") + ": " + document.getString("age"));
                });
        System.out.println();

        System.out.println("Список курсов самых старых студентов");
        BsonDocument query6 = BsonDocument.parse("{age: '" + oldAge + "'}");
        collection.find(query6).forEach((Consumer<Document>) document -> {
            System.out.println(document.getString("name") + ": " + document.getString("courses"));
        });
        System.out.println();

    }

    public static List<String[]>  getStudentsInfoFromCSV (String pathToTheFile) throws Exception {

        FileReader filereader = new FileReader(pathToTheFile);
        CSVReader csvReader = new CSVReader(filereader, ',', '"', '\'', 1);
        List<String[]> list = csvReader.readAll();
        return list;
    }
}
