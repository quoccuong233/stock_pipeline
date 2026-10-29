with du_lieu_bo_sung as (
    select
        ma_co_phieu,
        cast(thoi_gian_thi_truong as date) as ngay_giao_dich,
        gia_thap_nhat_ngay,
        gia_cao_nhat_ngay,
        gia_hien_tai,
        -- giá mở cửa thật của ngày lấy từ API, không phụ thuộc lúc producer bắt đầu chạy
        gia_mo_cua_ngay,
        last_value(gia_hien_tai) over (
            partition by ma_co_phieu, cast(thoi_gian_thi_truong as date)
            order by thoi_gian_thi_truong
            rows between unbounded preceding and unbounded following
        ) as gia_dong_cua_nen
    from {{ ref('int_data') }}
),

nen_gia as (
    select
        ma_co_phieu,
        ngay_giao_dich as thoi_gian_nen,
        min(gia_thap_nhat_ngay) as gia_thap_nhat,
        max(gia_cao_nhat_ngay) as gia_cao_nhat,
        any_value(gia_mo_cua_ngay) as gia_mo_cua,
        any_value(gia_dong_cua_nen) as gia_dong_cua,
        avg(gia_hien_tai) as duong_xu_huong
    from du_lieu_bo_sung
    group by ma_co_phieu, ngay_giao_dich
),

xep_hang as (
    select
        n.*,
        row_number() over (
            partition by ma_co_phieu
            order by thoi_gian_nen desc
        ) as thu_tu
    from nen_gia n
)

select
    ma_co_phieu,
    thoi_gian_nen,
    gia_thap_nhat,
    gia_cao_nhat,
    gia_mo_cua,
    gia_dong_cua,
    duong_xu_huong
from xep_hang
where thu_tu <= 12
order by ma_co_phieu, thoi_gian_nen
