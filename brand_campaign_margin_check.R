#------LOAD LIBRARIES & MISC.------------
library(tidyr)
library(googlesheets4)
library(openxlsx)
path_fun_and_pkg <- "C:\\Users\\nathan.moore\\Documents\\Critical_Lists\\critical_lists_fun_and_pkg.R"
source(path_fun_and_pkg)

#------DATABASE CONNECTION---------------------------------------------------------------------------
db_config <- config::get("dataconnection")
con <- dbConnect(PostgreSQL(), dbname = db_config$dbname, host=db_config$dbhost, port=db_config$dbport, user=db_config$dbuser, password=db_config$dbpwd)
#postgresqlpqExec(con, "SET client_encoding = 'windows-1252'") #to have oe and ue in R

#------SET UP--------------
operation_id <- 2776
sales_channel_sim <- "www_k24_de"
sales_channel_code <- " = 'www-k24-de'"
brand <- " = 'gates'" # "like '%becker%'" or " in ('gkn', 'spidan')"
brand_kvi <- "gates"
brand_blacklist <- " = 'GATES'" # str_to_upper(brand) or " = 'MEYLE'"

#------SQL QUERY--------
sql_margin_check <- str_replace_all(
  paste0("select sim.subcategory_name, count(sim.product_number) as count_ids, round(avg(sim.gross_margin)::numeric,2) as avg_margin, min(sim.gross_margin) as min_margin
from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim
where sim.brand_name ", brand, "
group by sim.subcategory_name
order by count(sim.product_number) desc
"
  ),"\n", " ")

sql_low_margin_check <- str_replace_all(
  paste0("select sim.subcategory_name, count(sim.product_number) as count_ids, round(avg(sim.gross_margin)::numeric,2) as avg_margin, min(sim.gross_margin) as min_margin
from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim
where sim.brand_name ", brand, "
	and sim.gross_margin <= 35
group by sim.subcategory_name
order by count(sim.product_number) desc
"
  ),"\n", " ")

sql_low_margin_product_check <- str_replace_all(
  paste0("select sim.product_number, sim.subcategory_name, sim.fixed_price, sim.min_price, sim.max_price, sim.gross_margin
from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim
where sim.brand_name ", brand, "
	and sim.gross_margin <= 35
"
  ),"\n", " ")

sql_check_blacklist <- str_replace_all(
  paste0("select pdh.brand_name, count(b.product_number)
from pricing_data.sim_param_product_blacklist as b 
join pricing_data_pdh_product.product_master_data as pdh
on b.product_number = pdh.product_number
where pdh.brand_name ", brand_blacklist,"
group by pdh.brand_name
"
  ),"\n", " ")

sql_check_minmax <- str_replace_all(
  paste0("SELECT p.product_number, r.reason, p.min_max_price_comment 
FROM pricing_data.sim_param_product_parameters as p	
join pricing_data.sim_param_reason as r on r.id = p.min_max_price_reason_id
join pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim on p.product_number = sim.product_number 
where p.sales_channel_code ", sales_channel_code, "
and (p.minimum_price is not null or p.maximum_price is not null)
and sim.brand_name ", brand,"
"
  ),"\n", " ")

sql_check_fixed <- str_replace_all(
  paste0("SELECT p.product_number, r.reason, p.fixed_price_comment 
FROM pricing_data.sim_param_product_parameters as p	
join pricing_data.sim_param_reason as r on r.id = p.fixed_price_reason_id 
join pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim on p.product_number = sim.product_number 
where p.sales_channel_code ", sales_channel_code, "
and p.fixed_price is not null
and sim.brand_name ", brand,"
and p.product_number in (
	select product_number
	from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, "
	where fixed_price is not null
	and brand_name ", brand,")
"
  ),"\n", " ")

sql_brand_genarts <- str_replace_all(
  paste0("select pdh.genart_number, pdh.genart_name, count(sim.product_number), round(avg(sim.gross_margin)::numeric,2) as avg_margin
, min(sim.gross_margin) as min_margin
, max(sim.gross_margin) as max_margin
from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim 
join (pricing_data_pdh_product.product_master_data pmd 
      join pricing_data_pdh_product.product_master_data_genarts pmdg 
      on pmd.id = pmdg.product_id_fk) as pdh
on sim.product_number = pdh.product_number
where sim.brand_name ", brand,"
group by pdh.genart_number, pdh.genart_name
order by count(sim.product_number) desc
"
  ),"\n", " ")

sql_brand_product_genart <- str_replace_all(
  paste0("select pdh.product_number
, pdh.main_category_name
, pdh.category_name 
, pdh.sub_category_name 
, g.genart_number
, g.genart_name
from pricing_data_pdh_product.product_master_data as pdh
join pricing_data_pdh_product.product_master_data_genarts as g
on pdh.id = g.product_id_fk
where pdh.product_number in (
	select product_number
	from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, "
	where brand_name ", brand,")
"
  ),"\n", " ")

sql_product_margin <- str_replace_all(
  paste0("select sim.product_number, sim.gross_margin
from pricing_data.sim_price_", operation_id, "_", sales_channel_sim, " as sim
where sim.brand_name ", brand, "
group by sim.product_number, sim.gross_margin
"
  ),"\n", " ")


#------GET DATA---------------------------------------------
kvi_data <- read_sheet("https://docs.google.com/spreadsheets/d/1vbShTVN4-f94aVZORsq5uZDKy7_nyanU0pKTa1c/edit?gid=517535#gid=517535",sheet = "current_KVIs")
brand_margin_check <-  dbFetch(dbSendQuery(conn = con, sql_margin_check))
brand_low_margin_check <-  dbFetch(dbSendQuery(conn = con, sql_low_margin_check))
brand_low_margin_product_check <-  dbFetch(dbSendQuery(conn = con, sql_low_margin_product_check))
brand_check_blacklist <- dbFetch(dbSendQuery(conn = con, sql_check_blacklist))
brand_check_minmax <- dbFetch(dbSendQuery(conn = con, sql_check_minmax))
brand_check_fixed <- dbFetch(dbSendQuery(conn = con, sql_check_fixed))
brand_genarts_check <-  dbFetch(dbSendQuery(conn = con, sql_brand_genarts))
brand_product_genart <- dbFetch(dbSendQuery(conn = con, sql_brand_product_genart))
brand_product_margin <- dbFetch(dbSendQuery(conn = con, sql_product_margin))

kvi_data_brand_filtered <- kvi_data %>%
  filter(brand_name == brand_kvi)

nr_brand_kvis <-kvi_data_brand_filtered %>%
  summarise(nr_skus = n())

brand_joined_margin_check <- brand_margin_check %>%
  left_join(brand_low_margin_check, by = "subcategory_name", suffix = c(".ttl", ".low")) %>%
  mutate(low_share = round(count_ids.low/count_ids.ttl,4))

brand_joined_low_margin_product_check <- brand_low_margin_product_check %>%
  left_join(brand_check_minmax, by = "product_number") %>%
  left_join(brand_check_fixed, by = "product_number", suffix = c(".minmax", ".fixed"))

current_allocation <- fread("C:\\Users\\nathan.moore\\Documents\\discounting_allocation.csv", select = c(1,3,13))
current_allocation <- current_allocation %>%
  mutate(recommended_discount = str_replace(recommended_discount, "high_min", "low_mid")) %>%
  mutate(recommended_discount = str_replace(recommended_discount, "gen", "mid")) %>%
  rename(current_discount_group = recommended_discount)

brand_genarts_discount <- brand_genarts_check %>%
  left_join(current_allocation, by = c("genart_number" = "generic_article_number"))

brand_genarts_discount_pivot <- brand_genarts_discount %>%
  group_by(current_discount_group) %>%
  summarise(sum = sum(count, na.rm = TRUE)) %>%
  mutate(share = round(sum / sum(sum)*100,1))

nr_products <- brand_product_genart %>%
  summarise(nr_skus = n())

nr_uni <- brand_product_genart %>%
  summarise(nr_uni_skus = n_distinct(product_number))

products <- brand_product_genart %>%
  left_join(brand_product_margin, by = "product_number") %>%
  left_join(current_allocation, by = c("genart_number" = "generic_article_number")) %>%
  select(-main_category_name.y)

products_genart_pivot <- products %>%
  group_by(current_discount_group) %>%
  summarise(nr_uni_skus = n_distinct(product_number),
            nr_skus = n()) %>%
  mutate(uni_share = round(nr_uni_skus/sum(nr_uni_skus)*100,1),
         share = round(nr_skus/sum(nr_skus)*100,1))

products_subcat_genart_pivot <- products %>%
  group_by(sub_category_name, current_discount_group) %>%
  summarise(nr_uni_skus = n_distinct(product_number),
            nr_skus = n()) %>%
  mutate(uni_share = round(nr_uni_skus/sum(nr_uni_skus)*100,1),
         share = round(nr_skus/sum(nr_skus)*100,1))

path_brand <- str_replace_all(brand_check_blacklist %>%
                                select(brand_name), pattern=" ", repl="")
fname <- paste(path_brand,"_margin_check_sim",operation_id,"_",Sys.Date(), sep = "")
my_path <- paste("C:\\Users\\nathan.moore\\Downloads\\",fname,".xlsx", sep = "")

wb <- createWorkbook()
addWorksheet(wb, "margin_check")
addWorksheet(wb, "low_margin_product_check")
addWorksheet(wb, "on_blacklist")
addWorksheet(wb, "on_KVI_list")
addWorksheet(wb, "brand_genarts_discounts")
addWorksheet(wb, "brand_genarts_discounts_pivot")
addWorksheet(wb, "products")
addWorksheet(wb, "products_genart_pivot")
addWorksheet(wb, "products_subcat_genart_pivot")

writeData(wb, "margin_check", brand_joined_margin_check, startRow = 1, startCol = 1)
writeData(wb, "low_margin_product_check", brand_joined_low_margin_product_check, startRow = 1, startCol = 1)
writeData(wb, "on_blacklist", brand_check_blacklist, startRow = 1, startCol = 1)
writeData(wb, "on_KVI_list", nr_brand_kvis, startRow = 1, startCol = 1)
writeData(wb, "brand_genarts_discounts", brand_genarts_discount, startRow = 1, startCol = 1)
writeData(wb, "brand_genarts_discounts_pivot", brand_genarts_discount_pivot, startRow = 1, startCol = 1)
writeData(wb, "products", products, startRow = 1, startCol = 1)
writeData(wb, "products_genart_pivot", products_genart_pivot, startRow = 1, startCol = 1)
writeData(wb, "products_subcat_genart_pivot", products_subcat_genart_pivot, startRow = 1, startCol = 1)

saveWorkbook(wb, file = my_path, overwrite = TRUE)

#------NOTES-------------------------------------------------
# 1 product can belong to more than 1 genart --> revenue and sold items contain duplicates

