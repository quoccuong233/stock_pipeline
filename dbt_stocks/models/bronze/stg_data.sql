SELECT
    du_lieu:c::float AS gia_hien_tai,
    du_lieu:d::float AS muc_thay_doi,
    du_lieu:dp::float AS phan_tram_thay_doi,
    du_lieu:h::float AS gia_cao_nhat_ngay,
    du_lieu:l::float AS gia_thap_nhat_ngay,
    du_lieu:o::float AS gia_mo_cua_ngay,
    du_lieu:pc::float AS gia_dong_cua_truoc,
    du_lieu:t::timestamp AS thoi_gian_thi_truong,
    du_lieu:symbol::string AS ma_co_phieu,
    du_lieu:fetched_at::timestamp AS thoi_gian_lay_du_lieu
FROM {{ source('raw', 'bao_gia_co_phieu_tho') }}
