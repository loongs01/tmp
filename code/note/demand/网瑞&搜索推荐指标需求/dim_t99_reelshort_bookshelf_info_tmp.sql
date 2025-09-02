replace into dim_data.dim_t99_reelshort_bookshelf_info
select
      shelf_id       -- 页面key
      ,shelf_name_cn -- 页面中文
      ,created_at    -- 创建时间
      ,updated_at    -- 更新时间
from ana_data.dim_t99_reelshort_bookshelf_info
;