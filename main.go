package main

import (
	"flag"
	"time"
	"github.com/golang/glog"
	"bytes"
	"github.com/elastic/go-elasticsearch/esapi"
	"strings"
	"encoding/json"
	"github.com/elastic/go-elasticsearch"
	"fmt"
	"context"
	"strconv"
	"os"
	"os/signal"
	"syscall"
)

var sapLicenceMap = map[string]string{
	"9057": "350206200102",
	"9073": "350206200098",
	"9154": "350205200064",
	"9071": "350203200010",
	"9111": "350203200719",
	"9195": "350206200356",
	"9499": "350206200632",
	"9122": "350205200060",
	"9423": "350212200494",
	"9512": "350211200230",
	"9293": "350211200127",
	"9840": "350206208126",
	"9N74": "350206208209",
	"9NG4": "350206208404",
	"9NS5": "350205204783",
	"9D81": "350203208597",
	"9D78": "350203208607",
	"9DA1": "350203208636",
	"9DAV": "350203208651",
	"9DAJ": "350203208660",
	"9DB8": "350203208655",
	"9DBK": "350203208695",
	"9DBO": "350206207400",
	"9DBT": "350203208682",
	"9DDC": "350203208736",
	"9DEB": "350206207478",
	"9DEL": "350203208810",
	"9DEJ": "350203208831",
	"9DEQ": "350203208900",
	"9DEP": "350203208904",
	"9DF6": "350203209005",
	"9DFM": "350206207727",
	"9DFR": "350205204429",
	"9DFW": "350206207789",
	"9DG8": "350205204444",
	"9DGV": "350205204447",
	"9DIE": "350205204460",
	"9DHY": "350206207897",
	"9DJW": "350203209165",
	"9DK4": "350206207924",
	"9DKU": "350206208122",
	"9DKN": "350206208172",
}

var (
	defaultIndexName = "xmtc_stock_test_20190710"

	DefaultShopId 	= "9100"
	DefaultGoodsId = "1"

	testShops = []string{
		"9396",
		"9009",
		"9081",
		"9108",
		"9180",
		"9207",
		"9306",
		"9405",
		"9504",
		"9559",
		"9603",
		"9658",
		"9702",
		"9757",
		"9801",
		"9N72",
		"9NI4",
		"9NN9",
		"9NV1",
		"9P70",
	}

	catgMiddIdXiangYan = "123301"
	cityCodeXiamen = "350200"

	statusSalesReturn 		= "6"
	statusSalesStop		 	= "7"
)

type StockBody struct {
	CustLicenceCode string `json:"CUST_LICENCE_CODE"`
	StockTime       string `json:"STOCK_TIME"`
	ProductSort     string `json:"PRODUCT_SORT"`
	ProdctCode      string `json:"PRODUCT_CODE"`
	ProductName     string `json:"PRODUCT_NAME"`
	SKU             string `json:"SKU"`
	UnitCode        string `json:"UNIT_CODE"`
	SpecName        string `json:"SpecName"`
	QTYStock        string `json:"QTY_STOCK"`
	QTYPurchace     string `json:"QTY_PURCHACE"`
}

type GoodsHit struct {
	Index  string  `json:"_index"`
	Type   string  `json:"_type"`
	ID     string  `json:"_id"`
	Score  float64 `json:"_score"`
	Source struct {
		ShopGoods         string      `json:"shop_goods"`
		BusiGrpID         string      `json:"busi_grp_id"`
		BusiGrpName       string      `json:"busi_grp_name"`
		SalesDist         string      `json:"sales_dist"`
		SalesDistName     string      `json:"sales_dist_name"`
		RegionIDNew       string      `json:"region_id_new"`
		RegionNameNew     string      `json:"region_name_new"`
		ProvCode          string      `json:"prov_code"`
		ProvName          string      `json:"prov_name"`
		CityCode          string      `json:"city_code"`
		CityName          string      `json:"city_name"`
		ShopID            string      `json:"shop_id"`
		ShopName          string      `json:"shop_name"`
		VendorID          string      `json:"vendor_id"`
		VendorName        string      `json:"vendor_name"`
		FirmID            string      `json:"firm_id"`
		FirmName          string      `json:"firm_name"`
		DivID             string      `json:"div_id"`
		DivName           string      `json:"div_name"`
		DeptID            string      `json:"dept_id"`
		DeptName          string      `json:"dept_name"`
		CatgLID           string      `json:"catg_l_id"`
		CatgLName         string      `json:"catg_l_name"`
		CatgMID           string      `json:"catg_m_id"`
		CatgMName         string      `json:"catg_m_name"`
		CatgSID           string      `json:"catg_s_id"`
		CatgSName         string      `json:"catg_s_name"`
		Goodsid           string      `json:"goodsid"`
		Goodsname         string      `json:"goodsname"`
		Brand             string      `json:"brand"`
		BrandName         string      `json:"brand_name"`
		OperationTypeID   string      `json:"operation_type_id"`
		OperationTypeName string      `json:"operation_type_name"`
		EfctSignID        string      `json:"efct_sign_id"`
		EfctSignName      string      `json:"efct_sign_name"`
		ShopGoodsStsID    string      `json:"shop_goods_sts_id"`
		ShopGoodsStsName  string      `json:"shop_goods_sts_name"`
		CycleUnitPrice    float64     `json:"cycle_unit_price"`
		Dms               float64     `json:"dms"`
		ProdArea          interface{} `json:"prod_area"`
		SalePrice         interface{} `json:"sale_price"`
		Unit              string      `json:"unit"`
		Standard          string      `json:"standard"`
		BarCode           string      `json:"bar_code"`
		PkgPcsL           string      `json:"pkg_pcs_l"`
		Shelflife         string      `json:"shelflife"`
		BravoRegionID     string      `json:"bravo_region_id"`
		BravoRegionName   string      `json:"bravo_region_name"`
		ShopBelong        string      `json:"shop_belong"`
		ShopBelongDesc    string      `json:"shop_belong_desc"`
		ZoneID            string      `json:"zone_id"`
		ZoneName          string      `json:"zone_name"`
		BdID              string      `json:"bd_id"`
		BdName            string      `json:"bd_name"`
		FirmG1ID          string      `json:"firm_g1_id"`
		FirmG1Name        string      `json:"firm_g1_name"`
		FirmG2ID          string      `json:"firm_g2_id"`
		FirmG2Name        string      `json:"firm_g2_name"`
		InsertTime        string      `json:"insert_time"`
		PurTaxRate        float64     `json:"pur_tax_rate"`
		CostPrice         float64     `json:"cost_price"`
		SaleTaxRate       float64     `json:"sale_tax_rate"`
		VendorNameNew     string      `json:"vendor_name_new"`
		VendorIDNew       string      `json:"vendor_id_new"`
		Repertory 		  float64     `json:"repertory,omitempty"`
		ShopLicense 	  string 	  `json:"shop_license,omitempty"`
	} `json:"_source"`
}

type GoodsInfoHits struct {
	Total    int     	`json:"total"`
	MaxScore float64 	`json:"max_score"`
	Hits     []GoodsHit `json:"hits"`
}

type GoodsInfoResponse struct {
	Hits		GoodsInfoHits	`json:"hits"`
}

type GoodsInfoScrollResponse struct {
	ScrollId 	string 			`json:"_scroll_id"`
	Hits		GoodsInfoHits	`json:"hits"`
}

type RepertoryHits struct {
	Total    int     `json:"total"`
	MaxScore float64 `json:"max_score"`
	Hits     []struct {
		Index  string  `json:"_index"`
		Type   string  `json:"_type"`
		ID     string  `json:"_id"`
		Score  float64 `json:"_score"`
		Source struct {
			Store       string  `json:"store"`
			Items       string  `json:"items"`
			ChannelMark string  `json:"channel_mark"`
			Key         string  `json:"key"`
			Storage     string  `json:"storage"`
			Timestamp   int64   `json:"timestamp"`
			Repertory   float64 `json:"repertory"`
		} `json:"_source"`
	} `json:"hits"`
}

type RepertoryResponse struct {
	Hits 	 RepertoryHits	`json:"hits"`
}

func getRepertory(ctx context.Context, shopId, goodsId string) (float64, error) {

	esConfig2 := elasticsearch.Config{
		Addresses: []string{"http://10.0.31.133:9546"},
	}
	esClient2, err := elasticsearch.NewClient(esConfig2)
	if err != nil {
		glog.V(2).Infof("Error creating the client: %s", err)
		return 0, err
	}
	var buffer2 bytes.Buffer
	query2 := map[string]interface{} {
		"query" : map[string]interface{} {
			"bool" : map[string]interface{} {
				"must" : []map[string]interface{} {
					{
						"term" : map[string]interface{} {
							"store" : fmt.Sprintf("%s", shopId),
						},
					},
					{
						"term" : map[string]interface{} {
							"items" : fmt.Sprintf("%s", goodsId),
						},
					},
					{
						"term" : map[string]interface{} {
							"storage" : "0001",
						},
					},
				},
			},
		},
	}
	if err := json.NewEncoder(&buffer2).Encode(query2); err != nil {
		glog.V(2).Infof("error encoding query: %s", err)
		return 0, err
	}
	index2 := "new_stocklevel"

	res2, err := esClient2.Search(
		esClient2.Search.WithContext(ctx),
		esClient2.Search.WithIndex(index2),
		esClient2.Search.WithBody(&buffer2),
		esClient2.Search.WithTrackTotalHits(true),
		esClient2.Search.WithPretty(),
		//esClient.Search.WithScroll(10 * time.Minute),
	)
	if err != nil {
		glog.V(2).Infof("search error : %v", err)
		return 0, err
	}
	defer res2.Body.Close()
	glog.V(5).Infof("res %v", res2)

	var repertoryResp RepertoryResponse
	if err := json.NewDecoder(res2.Body).Decode(&repertoryResp); err != nil {
		glog.V(2).Infof("decode RepertoryResponse failed: %v", err)
		return  0, err
	}
	glog.V(5).Infof("%+v", repertoryResp)
	if repertoryResp.Hits.Total < 1 {
		glog.V(5).Infof("shop: %s goods: %s has invalid repertory result: %d",
			shopId, goodsId, repertoryResp.Hits.Total)
		return 0, nil
	}

	return repertoryResp.Hits.Hits[0].Source.Repertory, nil
}

func streamFormat(stop chan interface{}, input chan interface{}) chan interface{} {
	outStream := make(chan interface{})
	go func() {
		defer close(outStream)
		for {
			select {
			case <-stop:
				return
			case obj, ok := <-input:
				if !ok {
					return
				}

				hit, ok := obj.(GoodsHit)
				if !ok {
					glog.V(2).Infof("invalid input obj: %v", obj)
					continue
				}
				stock, err := transferHitToStockBody(hit)
				if err != nil {
					glog.V(2).Infof("transferHitToStockBody failed: %v", err)
					continue
				}
				outStream <- stock
			}
		}
	}()

	return outStream
}

func streamStock(stop chan interface{}, input chan interface{}) chan interface{} {
	outStream := make(chan interface{})

	go func() {
		defer close(outStream)
		ctx := context.Background()

		for {
			select {
			case <-stop:
				ctx.Done()
				return
			case obj, ok := <-input:
				if !ok {
					return
				}

				hit, ok := obj.(GoodsHit)
				if !ok {
					glog.V(2).Infof("invalid input hit: %v", obj)
					continue
				}
				repertory, err := getRepertory(ctx, hit.Source.ShopID, hit.Source.Goodsid)
				if err != nil {
					glog.V(2).Infof("getRepertory failed: %v", err)
					return
				}
				hit.Source.Repertory = repertory
				outStream <- hit
			}
		}
	}()

	return outStream
}

func streamBatchShopGoods(stop chan interface{}, pageSize int) chan interface{} {
	outStream := make(chan interface{})
	esConfig := elasticsearch.Config{
		Addresses: []string{"http://10.0.56.207:9346"},
	}
	esClient, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		glog.V(2).Infof("Error creating the client: %s", err)
		panic(err)
	}

	go func() {
		defer close(outStream)

		var buffer bytes.Buffer
		query := map[string]interface{}{
			"query": map[string]interface{}{
				"bool": map[string]interface{}{
					"must": []map[string]interface{}{
						{
							"term": map[string]interface{}{
								"catg_m_id": fmt.Sprintf("%s", catgMiddIdXiangYan),
							},
						},
						{
							"term": map[string]interface{}{
								"city_code": fmt.Sprintf("%s", cityCodeXiamen),
							},
						},
					},
					"must_not": []map[string]interface{}{
						{
							"term": map[string]interface{}{
								"shop_goods_sts_id": fmt.Sprintf("%s", statusSalesReturn),
							},
						},
						{
							"term": map[string]interface{}{
								"shop_goods_sts_id": fmt.Sprintf("%s", statusSalesStop),
							},
						},
						{
							"term": map[string]interface{}{
								"shop_goods_sts_id": fmt.Sprintf("%s", "9"),
							},
						},
					},
				},
			},
			"from": 0,
			"size": pageSize,
		}
		if err := json.NewEncoder(&buffer).Encode(query); err != nil {
			glog.V(2).Infof("error encoding query: %s", err)
			panic(err)
		}

		index := "hive_to_es_dim_shop_goods_latest_new"
		res, err := esClient.Search(
			esClient.Search.WithContext(context.Background()),
			esClient.Search.WithIndex(index),
			esClient.Search.WithBody(&buffer),
			esClient.Search.WithTrackTotalHits(true),
			esClient.Search.WithPretty(),
			esClient.Search.WithScroll(10 * time.Minute),
		)
		if err != nil {
			glog.V(2).Infof("search error : %v", err)
			panic(err)
		}
		defer res.Body.Close()
		glog.V(5).Infof("res %v", res)

		var scrollResp GoodsInfoScrollResponse
		if err := json.NewDecoder(res.Body).Decode(&scrollResp); err != nil {
			glog.V(2).Infof("decode body failed: %v", err)
			panic(err)
		}
		outStream <- scrollResp.Hits

		totalPage := scrollResp.Hits.Total / pageSize
		glog.V(2).Infof("totalPage: %d", totalPage)
		ctx := context.Background()
		for page := 0; page < totalPage; page++ {
			select {
			case <-stop:
				ctx.Done()
				return
			default:
				res, err := esClient.Scroll(
					esClient.Scroll.WithContext(ctx),
					esClient.Scroll.WithScrollID(scrollResp.ScrollId),
					esClient.Scroll.WithScroll(10 * time.Minute))
				if err != nil {
					glog.V(2).Infof("scroll error: %v", err)
					return
				}
				defer res.Body.Close()
				var resp GoodsInfoResponse
				if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
					glog.V(2).Infof("decode error: %v", err)
					return
				}
				outStream <- resp.Hits
			}
		}
	}()

	return outStream
}

func streamLicense(stop chan interface{}, input chan interface{}, licenseMap map[string]string, batchSize int) chan interface{} {
	outStream := make(chan interface{})
	go func() {
		defer close(outStream)

		for {
			select {
			case <-stop:
				return
			case obj, ok := <-input:
				if !ok {
					return
				}

				batchHits, ok := obj.(GoodsInfoHits)
				if !ok {
					glog.V(2).Infof("input batch invalid.")
					continue
				}
				for _, hit := range batchHits.Hits {
					license := licenseMap[hit.Source.ShopID]
					if license == "" {
						glog.V(2).Infof("shop %s have not license.", hit.Source.ShopID)
					} else {
						hit.Source.ShopLicense = license
						outStream <- hit
					}
				}
			}
		}
	}()

	return outStream
}

func streamInsertStock(stop chan interface{}, input chan interface{}) chan interface{} {
	outStream := make(chan interface{})
	go func() {
		defer close(outStream)
		esConfig := elasticsearch.Config{
			Addresses: []string{"http://10.0.66.60:9200"},
		}
		esClient, err := elasticsearch.NewClient(esConfig)
		if err != nil {
			glog.V(2).Infof("Error creating the client: %s", err)
			panic(err)
		}

		for {
			select {
			case <-stop:
				return
			case obj, ok := <-input:
				if !ok {
					return
				}
				stock, ok := obj.(StockBody)
				if !ok {
					glog.V(2).Infof("invalid input obj: %v", obj)
					continue
				}
				var buffer bytes.Buffer
				if err := json.NewEncoder(&buffer).Encode(stock); err != nil {
					glog.V(2).Infof("encode error: %v", err)
					continue
				}
				req := esapi.IndexRequest{
					Index:      defaultIndexName,
					DocumentType: "type1",
					Body:       strings.NewReader(buffer.String()),
					Refresh:    "true",
					Pretty: 	true,
				}
				res, err := req.Do(context.Background(), esClient)
				if err != nil {
					glog.V(2).Infof("INSERT data failed: %v", err)
					continue
				}
				var data interface{}
				if json.NewDecoder(res.Body).Decode(&data); err != nil {
					glog.V(2).Infof("decode failed: %v", err)
					continue
				}
				glog.V(2).Infof("%v %v", res.StatusCode, data)
				outStream <- stock
				res.Body.Close()
			}
		}
	}()

	return outStream
}

func transferHitToStockBody(hit GoodsHit) (StockBody, error) {
	stock := StockBody{}
	repertory := strconv.FormatFloat(hit.Source.Repertory, 'f', -1, 64)

	stock.CustLicenceCode = hit.Source.ShopLicense
	stock.ProdctCode = hit.Source.Goodsid
	stock.ProductName = hit.Source.Goodsname
	stock.SKU = hit.Source.BarCode
	stock.SpecName = hit.Source.Standard
	stock.UnitCode = hit.Source.Unit
	stock.QTYStock = repertory

	return stock, nil
}

func main() {
	flag.Parse()

	stop := make(chan interface{})
	defer close(stop)

	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGKILL|syscall.SIGTERM)

	go func() {
		<-sigCh
		close(stop)
	}()

	var num int
	var stocks []StockBody
	start := time.Now()
	for obj := range streamInsertStock(stop,
		streamFormat(stop, streamStock(stop, streamLicense(stop, streamBatchShopGoods(stop, 100), sapLicenceMap, 100)))) {
		num++
		stock := obj.(StockBody)
		stocks = append(stocks, stock)
	}
	for id, item := range stocks {
		glog.V(2).Infof("[%d] %v", id, item)
	}
	glog.V(2).Infof("valid data: %d spend %v", num, time.Since(start))
}