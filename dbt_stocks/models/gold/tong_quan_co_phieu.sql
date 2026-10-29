select
    ma_co_phieu,
    gia_hien_tai,
    muc_thay_doi,
    phan_tram_thay_doi
from (
    select *,
           row_number() over (partition by ma_co_phieu order by thoi_gian_lay_du_lieu desc) as thu_tu
    from {{ ref('int_data') }}
) t
where thu_tu = 1
