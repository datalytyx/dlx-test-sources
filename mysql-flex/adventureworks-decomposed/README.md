# To generate
Run up a mysql instance of adventureworks on port 4002
Run
```
mysqldump --host 127.0.0.1 --port 4002 --user=datalytyx --password=horsewelltree adventureworks purchaseorderdetail purchaseorderheader vendor shipmethod productvendor > purchasing.sql
mysqldump --host 127.0.0.1 --port 4002 --user=datalytyx --password=horsewelltree adventureworks productreview billofmaterials transactionhistory product productlistpricehistory productphoto productsubcategory productcosthistory productproductphoto productcategory productdocument productmodelillustration productmodel productdescription illustration productmodelproductdescriptionculture culture unitmeasure productinventory workorderrouting workorder scrapreason document > production.sql
mysqldump --host 127.0.0.1 --port 4002 --user=datalytyx --password=horsewelltree adventureworks salespersonquotahistory salesperson salesorderheader salesterritoryhistory salesterritory  store customer  creditcard currencyrate salestaxrate salesorderheadersalesreason shoppingcartitem currency salesreason salesorderdetail specialofferproduct specialoffer> sales.sql
mysqldump --host 127.0.0.1 --port 4002 --user=datalytyx --password=horsewelltree adventureworks department employeedepartmenthistory shift employee jobcandidate employeepayhistory > hr.sql
mysqldump --host 127.0.0.1 --port 4002 --user=datalytyx --password=horsewelltree adventureworks address stateprovince contacttype addresstype countryregion > crm.sql
```
