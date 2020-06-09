package  mongodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	log.SetFlags(log.Lshortfile)
}

type UpdateType int

const (
	UpdateSet           UpdateType = iota //修改指定字段值
	UpdateUnset                           //删除指定字段
	UpdateRename                          //重命名指定字段
	UpdateInc                             //对文档内数字字段增加指定值
	UpdateSlicePush                       //追加数组类型一个元素
	UpdateSlicePushAll                    //追加数组类型多个元素
	UpdateSliceAddToSet                   //追加数组不存在元素
	UpdateSlicePop                        //删除数组类型指定下标元素
	UpdateSlicePull                       //删除数组第一个符合条件的值
	UpdateSlicePullAll                    //删除数组所有符合条件的值

)


var	UpdateError  = errors.New("update conditions not met")

var updateTypes = []string{"$set", "$unset", "$rename", "$inc", "$push", "$pushAll", "$addToSet", "$pop", "$pull", "$pullAll"}

func (u UpdateType) String() string {
	if UpdateSet <= u && u <= UpdateSlicePullAll {
		return updateTypes[u]
	}

	return updateTypes[0]
}

type Client struct {
	Client         *mongo.Client //MongoDB 连接实例
	Addr           string        //MongoDB 连接地址
	DBName         string        //数据库名称
	ContextTimeout time.Duration //context执行时间(秒)
	MaxPoolSize    uint64        //连接池最大连接数量
}

//生成实例
func (db *Client) New() {

	config := new(options.ClientOptions)
	config.ApplyURI(db.Addr)
	config.SetMaxPoolSize(db.MaxPoolSize)

	c, err := mongo.NewClient(config)
	if err != nil {
		log.Printf("MongoDB 连接地址错误: %s", err.Error())
		os.Exit(9)
	}

	err = c.Connect(context.Background())
	if err != nil {
		log.Printf("MongoDB 建立连接失败: %s", err.Error())
		os.Exit(9)
	}

	err = c.Ping(context.Background(), nil)
	if err != nil {
		log.Printf("MongoDB 测试连接失败: %s", err.Error())
		os.Exit(9)
	}

	db.Client = c
}

//选择指定数据库和集合
func (db *Client) SwitchCollection(ctx context.Context, collection string) *mongo.Collection {
	return db.Client.Database(db.DBName).Collection(collection)
}

//查询单条数据
func (db *Client) FindOne(collectionName string, filter, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)
	return collection.FindOne(ctx, filter).Decode(result)
}

//查询多个文档
func (db *Client) FindMany(collectionName string, filter bson.M, limit int64, skip int64) (resultRaw []bson.Raw, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	findOptions := options.Find()
	findOptions.SetLimit(limit)
	findOptions.SetSkip(skip)
	findOptions.SetSort(bson.M{})


	cur, err := collection.Find(ctx, filter, findOptions)
	for cur.Next(ctx) {
		resultRaw = append(resultRaw, cur.Current)
	}

	return resultRaw, nil
}

//查询多个文档投影返回指定字段
func (db *Client) FindManyProject(collectionName string, filter bson.M, resultKeys []string, limit int64, skip int64) (resultRaw []bson.Raw, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	projection := bson.D{}
	for _, v := range resultKeys{
		projection = append(projection, bson.E{Key: v, Value: 1})
	}

	findOptions := options.Find()
	findOptions.SetLimit(limit)
	findOptions.SetSkip(skip)
	findOptions.SetSort(bson.M{})
	findOptions.SetProjection(projection)

	cur, err := collection.Find(ctx, filter, findOptions)
	for cur.Next(ctx) {
		resultRaw = append(resultRaw, cur.Current)
	}

	return resultRaw, nil
}

//查询多个文档投影返回指定字段
func (db *Client) FindManyProjectSort(collectionName string, filter bson.M, resultKeys []string,sortKey string, order bool, limit int64, skip int64) (resultRaw []bson.Raw, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	projection := bson.D{}
	for _, v := range resultKeys{
		projection = append(projection, bson.E{Key: v, Value: 1})
	}

	orderN := -1
	if order {
		orderN = 1
	}


	findOptions := options.Find()
	findOptions.SetLimit(limit)
	findOptions.SetSkip(skip)
	findOptions.SetSort(bson.M{sortKey: orderN})
	findOptions.SetProjection(projection)

	cur, err := collection.Find(ctx, filter, findOptions)
	for cur.Next(ctx) {
		resultRaw = append(resultRaw, cur.Current)
	}

	return resultRaw, nil
}


//查询多个文档并且排序返回(order的值 true: 1 从小到大; false: -1 从大到小)
func (db *Client) FindManyAndSort(collectionName string, filter bson.M, sortKey string, order bool, limit int64, skip int64) (resultRaw []bson.Raw, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	orderN := -1
	if order {
		orderN = 1
	}

	findOptions := options.Find()
	findOptions.SetLimit(limit)
	findOptions.SetSkip(skip)
	findOptions.SetSort(bson.M{sortKey: orderN})

	cur, err := collection.Find(ctx, filter, findOptions)
	for cur.Next(ctx) {
		resultRaw = append(resultRaw, cur.Current)
	}

	return resultRaw, nil
}



//获取指定条件文档数量
func (db *Client) Count (collectionName string, filter bson.M) (count int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	return db.SwitchCollection(ctx, collectionName).CountDocuments(ctx, filter)
}

//获取整个集合文档数量
func (db *Client) AllDocumentsCount (collectionName string, ) (count int64, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	return db.SwitchCollection(ctx, collectionName).EstimatedDocumentCount(ctx)
}



//获取指定条件的随机一条数据
func (db *Client) RandomOne(collectionName string, value interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	pipeline := mongo.Pipeline{{{"$sample", bson.M{"size": 1}}}}

	cur, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		log.Println(err)
		return err
	}

	for cur.Next(ctx) {
		return cur.Decode(value)
	}
	return nil
}


//查询内嵌 Array/Slice 结构文档
func (db *Client) FindSlice(collectionName string, sliceName, key, value string, result interface{}) (err error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	filter := bson.M{sliceName: key}
	if value != "" {
		filter = bson.M{fmt.Sprintf("%s.%s", sliceName, key): value}
	}

	return collection.FindOne(ctx, filter).Decode(result)

}

//插入单条数据
func (db *Client) InsertOne(collectionName string, value interface{}) (*mongo.InsertOneResult, error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	return collection.InsertOne(ctx, value)
}

//插入多个文档
func (db *Client) InsertMany(collectionName string, value []interface{}) (*mongo.InsertManyResult, error) {

	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	return collection.InsertMany(ctx, value)
}

//更新指定单个文档值
func (db *Client) UpdateOne(collectionName string, filter bson.M, updater bson.M, upType UpdateType) (*mongo.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	return collection.UpdateOne(ctx, filter, bson.M{upType.String(): updater})
}

//更新并且返回指定单个文档值(原子操作)
func (db *Client) FindAndUpdateSetOne(collectionName string, filter bson.M, updater bson.M,upType UpdateType, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	ops := new(options.FindOneAndUpdateOptions)
	ops.SetReturnDocument(options.After)

	temp := collection.FindOne(ctx, filter)
	if temp.Err() == nil{
		return collection.FindOneAndUpdate(ctx, filter, bson.M{upType.String(): updater}, ops).Decode(result)
	}

	return UpdateError
}

//更新(同时执行set和inc)并且返回指定单个文档值(原子操作)
func (db *Client) FindAndUpdateSetInc(collectionName string, filter bson.M, updater bson.M,increase bson.M, result interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	ops := new(options.FindOneAndUpdateOptions)
	ops.SetReturnDocument(options.After)

	temp := collection.FindOne(ctx, filter)
	if temp.Err() == nil{
		return collection.FindOneAndUpdate(ctx, filter, bson.M{UpdateSet.String(): updater, UpdateInc.String():increase}, ops).Decode(result)
	}

	return UpdateError
}

//删除指定单个文档
func (db *Client) DeleteOne(collectionName string, filter bson.M) (*mongo.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	return collection.DeleteOne(ctx, filter)
}

//删除指定多个文档
func (db *Client) DeleteMany(collectionName string, filter bson.M) (*mongo.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	return collection.DeleteMany(ctx, filter)
}

//aggregate
func (db *Client) Aggregate (collectionName string, pipeline []bson.M) (resultRaw []bson.Raw, err error)  {
	ctx, cancel := context.WithTimeout(context.Background(), db.ContextTimeout*time.Second)
	defer cancel()

	collection := db.SwitchCollection(ctx, collectionName)

	opts := options.Aggregate()
	cur, err := collection.Aggregate(ctx, pipeline, opts)
	if err != nil{
		return resultRaw, err
	}
	for cur.Next(ctx) {
		resultRaw = append(resultRaw, cur.Current)
	}

	return resultRaw, nil
}