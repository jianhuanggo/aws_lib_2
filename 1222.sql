CREATE TABLE temp.MB_Singular_staging AS
with singular_etl as (
                      Select etl_query_timestamp_utc,
                             Skan_campaign_id,
                             date(date) as ds,
                             app,
                             upper(data_source) as data_source,
                             source,
                             OS,
                             x1.platform,
                             case 
                                when x1.unified_campaign_name ilike '%YouTubeMobile%' and x1.platform = 'Web' then 'ANDROID'
                                when x1.unified_campaign_name ilike '%YoutubeMasthead%YouTube%CTV%' then 'YOUTUBE CTV'
                                when source = 'Roku' then 'ROKU'
                                when source IN ('Roku NG', 'Roku FTV') and date(date) >= '2024-06-01' then 'ROKU'
                                when (source = 'Amazon Media Group' or x1.unified_campaign_name like '%AMZN%') and (x1.unified_campaign_name like '%Fire-Tablet%' or x1.unified_campaign_name like '%FireTablet%') then 'FIRETABLET'
                                when source = 'Amazon Media Group' or x1.unified_campaign_name like '%AMZN%' then 'AMAZON'
                                when source = 'Xbox' then 'XBOXONE'
                                when source = 'Vizio' then 'VIZIO'
                                when upper(source) = 'PLAYSTATION' then 'PLAYSTATION'
                                when upper(source) = 'PS4' then 'PS4'
                                when upper(source) = 'PS5' then 'PS5'
                                when upper(source) = 'SAMSUNG' then 'SAMSUNG'
                                when upper(source) = 'LG ADS' then 'LGTV'
                                when upper(source) = 'ANDROIDTV' then 'ANDROIDTV'
                                when x1.platform = 'Android' then 'ANDROID'
                                when x1.platform = 'iOS' then 'IOS_MOBILE'
                                when x1.unified_campaign_name ilike '%YouTube%CTV%' then 'YOUTUBE CTV'
                                when x1.unified_campaign_name ilike '%YouTube%VAC%' and x1.platform='TV' then 'YOUTUBE CTV'
                                when x1.platform = 'Web' then 'WEB'
                                when upper(source) = 'SNAPCHAT' then case when upper(x1.unified_campaign_name) like '%IOS%' then 'IOS_MOBILE'
                                                                            when upper(x1.unified_campaign_name) like '%AND%' then 'ANDROID'
                                                                            else upper(x1.platform)
                                                                    end
                                when x1.unified_campaign_name ilike '%samsung%' and source ilike '%dv360%' then 'SAMSUNG'
                                when upper(source) = 'COMCAST' then 'COMCAST' -- change v1
                                when upper(source) = 'HISENSE' then 'HISENSE' -- change v1
                                when upper(source) = 'VALNET' then 'WEB' -- change v1
                                when upper(source) LIKE '%DV360%' and upper(x1.platform) = 'WINDOWS' THEN 'ANDROID' -- change v1
                                when upper(source) LIKE '%DV360%' and upper(x1.platform) = 'MAC OS' THEN 'IOS_MOBILE' -- change v1
                                when upper(platform) = 'DESKTOP' THEN 'WEB' -- change v1
                                else upper(x1.platform)
                            end as platform_V2,
                            upper(case when x1.unified_campaign_name ilike '%YouTubeMobile%' and x1.platform = 'Web' then 'Google App Campaigns'
                                        when x1.unified_campaign_name ilike '%YouTube%CTV%' then 'YOUTUBE CTV'
                                        when source = 'Aura ironSource' then 'Ironsource'
                                        when source = 'ironSource' then 'Ironsource Gaming'
                                        when source = 'TikTok Ads' then 'Tiktok'
                                        -- when platform_V2 = 'WEB' and source = 'AdWords' then 'Shannon to decide'
                                        when source = 'AdWords' then 'Google App Campaigns'
                                        when source = 'Amazon Media Group' then 'AMAZON'
                                        when platform_V2 = 'ROKU' then 'ROKU'
                                        when source = 'Xbox' then 'XBOXONE'
                                        when upper(source) = 'SINGLETAP BY DIGITAL TURBINE' then 'SINGLETAP'
                                        when upper(source) = 'LG ADS' then 'LGTV'
                                        when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' then 'Display & Video 360'
                                        else upper(source)
                                end) as channel_group,
                            case when x1.unified_campaign_name ilike '%YouTubeMobile%' and x1.platform = 'Web' then 'MOBILE'
                                when x1.unified_campaign_name ilike '%samsung%' and source ilike '%dv360%' then 'OTT'
                                when source ilike '%dv360%' and platform_v2 in ('OTHER','UNKNOWN') then 'MOBILE'
                                -- when channel_group ilike '%dv360%' and platform_v2 in ('MAC OS','WINDOWS') then 'WEB'
                                when source ilike '%dv360%' and UPPER(platform) in ('MAC OS','WINDOWS') then 'MOBILE' -- change v1
                                when upper(source) = 'LINEAR TV' then 'ALL'
                                when upper(platform_V2) like '%MOBILE%' then 'MOBILE'
                                when source = 'AdWords' and x1.unified_campaign_name ilike '%google_uac%' then 'MOBILE'
                                when source = 'AdWords' and x1.unified_campaign_name ilike '%google_youtube%VAC%' and x1.platform='Web' then 'WEB'
                                when source = 'AdWords' and x1.unified_campaign_name ilike '%google_youtube%' then 'OTT'
                                when Platform_V2 = 'XBOXONE' or channel_group = 'XBOXONE' then 'OTT'
                                when Platform_V2 = 'WEB' then 'WEB'
                                when Platform_V2 = 'FIRETABLET' then 'MOBILE'
                                when Platform_V2 = 'AMAZON' then 'OTT'
                                when Platform_V2 = 'TV' then 'OTT'
                                when Platform_V2 = 'PLAYSTATION' then 'OTT'
                                when Platform_V2 = 'PS4' then 'OTT'
                                when Platform_V2 = 'PS5' then 'OTT'
                                when Platform_V2 = 'ROKU' or channel_group = 'ROKU' then 'OTT'
                                when Platform_V2 = 'ANDROIDTV' then 'OTT'
                                when Platform_V2 = 'IPHONE' then 'MOBILE'
                                when Platform_V2 = 'VIZIO' or channel_group = 'VIZIO' then 'OTT'
                                when Platform_V2 = 'SAMSUNG' or channel_group = 'SAMSUNG' then 'OTT'
                                when Platform_V2 = 'YOUTUBE CTV' or channel_group = 'YOUTUBE CTV' then 'OTT'
                                when Platform_V2 = 'LGTV' and channel_group = 'LGTV' then 'OTT'
                                when Platform_V2 = 'ANDROID' then 'MOBILE'
                                when upper(source) = 'TIKTOK ADS' then 'MOBILE'
                                when platform_V2 = 'IPAD' then 'MOBILE'
                                when upper(source) IN ('COMCAST','HISENSE') THEN 'OTT' -- change v1
                                when upper(source) IN ('VALNET') THEN 'WEB' -- change v1
                                --when Platform_V2 = 'OTHER' and channel_group = 'FACEBOOK' then 'MOBILE'
                            end as platform_type,
                            trim(case when platform_V2 = 'ROKU' and DATE(DATE) >= '2022-07-05' then coalesce(trim(case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),'-',3),',',1)
                                                                                                               when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),'-',3),',',1)
                                                                                                          end),x3.two_digit_country,'US')
                                 when coalesce(trim(x1.country_field),'') = '' then 'US'
                                 when x3.two_digit_country is not null then x3.two_digit_country
                                 else x1.country_field
                            end) as country,
                            COALESCE(x3.two_digit_country,x1.country_field) as country_singular,
                            x1.unified_campaign_id  as campaign_id,
                            x1.unified_campaign_name as campaign_name,
                            CASE WHEN upper(source) = 'APPLE SEARCH ADS' THEN x1.keyword_id ELSE x1.creative_id END as creative_id,
                            CASE WHEN upper(source) = 'APPLE SEARCH ADS' THEN x1.keyword ELSE x1.creative_name END as creative_name,
                            x1.sub_campaign_id  as adgroup_id,
                            x1.sub_campaign_name as adgroup_name,
                            case when upper(data_source) = 'TUBI INTERNAL' then case when upper(source) in ('SAMSUNG','VIZIO','HISENSE') and upper(campaign_name) similar to '%BD%' then 'BD SPEND'
                                                                                     when upper(source) = 'LG ADS' and upper(campaign_name) similar to '%OOBE%|%LAUNCHER%' then 'BD SPEND'
                                                                                     when upper(source) in ('PLAYSTATION','XBOX') and upper(campaign_name) similar to '%(BD)%' then 'BD SPEND'
                                                                                     when DATE(DATE) >= '2023-08-01' and upper(campaign_name) like '%\\\_BD-%' then 'BD SPEND'
                                                                                     else  'GROWTH SPEND'
                                                                                END
                                 when channel_group ilike '%facebook%' and trim(adn_account_id) = '480572019780469' then 'ENGINEERING SPEND'
                                 when trim(adn_account_id) = '5480859652001757' then 'BRAND SPEND' -- change v1
                                 else 'GROWTH SPEND'
                            end as team_spend,
                            upper(case when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'-',1)
                                                                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',1) like '%:%' then split_part(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',1),':',2)
                                                                                                                                                                                         else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',1)
                                                                                                                                                                                    end
                                                                                                                          when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'-',1)
                                                                                                                     END
                                       when platform_V2 = 'ROKU' then 
                                        case when DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',2),'-',1)
                                             when DATE(DATE) >= '2022-07-05' then 
                                             case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),'-',1)
                                                  when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),'-',1)
                                             end
                                        END
                                       when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN split_part(upper(x1.unified_campaign_name),'-',1)
                                       when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN split_part(upper(x1.unified_campaign_name),'-',1)
                                       when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then trim(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'-',1))
                                       when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'-',1)
                                       when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-31' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'ADWORDS' then (
                                             case
                                                  when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then (
                                                       case
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',2)
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(regexp_replace(x1.unified_campaign_name,'zzz',''),'-',1)
                                                            else split_part(x1.unified_campaign_name,'-',1)
                                                       end
                                                  )
                                                  when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                       case
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',2)
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(regexp_replace(x1.unified_campaign_name,'zzz',''),'-',1)
                                                            else split_part(x1.unified_campaign_name,'-',1)
                                                       end
                                                  )
                                            end
                                       )
                                       when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 and x1.unified_campaign_name similar to '[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'LINEAR TV' then split_part(x1.unified_campaign_name,'-',1)
                                       when upper(source) = 'MOLOCO' then split_part(unified_campaign_name,'-',1)
                                  end) as campaign_City,
                            upper(case when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'-',2)
                                                                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',2)) = 'MULTI' and trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',3)) = 'STATE' then trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',2)) + trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',3))
                                                                                                                                                                                         else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',2)
                                                                                                                                                                                    end
                                                                                                                          when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'-',2)
                                                                                                                     END
                                       when platform_V2 = 'ROKU' THEN
                                       CASE  WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',2),'-',2)
                                             WHEN DATE(DATE) >= '2022-07-05' then 
                                                  case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),'-',2)
                                                  when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),'-',2)
                                                  end
                                        END
                                       when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN case when trim(split_part(upper(x1.unified_campaign_name),'-',2)) = 'MULTI' and trim(split_part(upper(x1.unified_campaign_name),'-',3)) = 'STATE' then split_part(upper(x1.unified_campaign_name),'-',2) + split_part(upper(x1.unified_campaign_name),'-',3)
                                                                                                            else split_part(upper(x1.unified_campaign_name),'-',2)
                                                                                                       end
                                       when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN split_part(upper(x1.unified_campaign_name),'-',2)
                                       when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'-',2)
                                       when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'-',2)
                                       when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-31' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'ADWORDS' then (
                                             case
                                                  when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then (
                                                       case
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',3)
                                                            else split_part(x1.unified_campaign_name,'-',2)
                                                       end
                                                  )
                                                  when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                       case
                                                            when x1.unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',3)
                                                            else split_part(x1.unified_campaign_name,'-',2)
                                                       end
                                                  )
                                             end
                                       )
                                       when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 and x1.unified_campaign_name similar to '[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'LINEAR TV' then split_part(x1.unified_campaign_name,'-',2)
                                       when upper(source) = 'MOLOCO' THEN split_part(unified_campaign_name,'-',2)
                                   end) as campaign_State,
                            upper(case when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'-',3),'_',1)
                                                                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',2)) = 'MULTI' and trim(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',3)) = 'STATE' then split_part(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',4),'_',1)
                                                                                                                                                                                         else split_part(split_part(split_part(upper(x1.unified_campaign_name),'|',2),'-',3),'_',1)
                                                                                                                                                                                    end
                                                                                                                          when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'-',3),'_',1)
                                                                                                                     END
                                       when platform_V2 = 'ROKU' THEN
                                       CASE  WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',2),'-',3) 
                                             WHEN DATE(DATE) >= '2022-07-05' then 
                                             case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),'-',3),',',1)
                                             when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),'-',3),',',1)
                                             end
                                             END
                                       when upper(source) = 'XBOX' then case when DATE(DATE) >= '2022-05-01' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN case when trim(split_part(upper(x1.unified_campaign_name),'-',2)) = 'MULTI' and trim(split_part(upper(x1.unified_campaign_name),'-',3)) = 'STATE' then split_part(split_part(upper(x1.unified_campaign_name),'-',4),'_',1)
                                                                                                                                                                               else split_part(split_part(upper(x1.unified_campaign_name),'-',3),'_',1)
                                                                                                                                                                          end
                                                                        end
                                       when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' and regexp_count(x1.unified_campaign_name, '\\\_') >= 7 THEN split_part(split_part(upper(x1.unified_campaign_name),'-',3),'_',1)
                                       when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'-',3),'_',1)
                                       when upper(source) = 'VIZIO' then case when DATE(DATE) >= '2023-09-01' then split_part(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',1),'-',3)
                                                                              when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then 'US' end
                                                                              when DATE(DATE) >= '2022-05-18' then split_part(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'-',3),'_',1)
                                                                              else null
                                                                         end
                                       when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when upper(source) = 'FACEBOOK' then case when DATE(DATE) >= '2022-10-31' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                                                                 when DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name like '%-%' then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                                                                 when DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',1)
                                                                                 when x1.unified_campaign_name like 'Avatar%Top%' then trim(split_part(split_part(x1.unified_campaign_name, 'Avatar', 2), 'Top', 1))
                                                                                 when x1.unified_campaign_name like 'GT\\_%' then split_part(x1.unified_campaign_name, '_', 4)
                                                                                 when trim(x1.unified_campaign_name) like '[CONACQ]%' then split_part(x1.unified_campaign_name, '|', 3)
                                                                            end
                                       when upper(source) = 'ADWORDS' then (
                                             case
                                                  when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then (
                                                       case
                                                            when unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(split_part(x1.unified_campaign_name,'-',4),'_',1)
                                                            else split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                                       end
                                                  )
                                                  when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                       case
                                                            when unified_campaign_name similar to 'zzz\\\s*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(split_part(x1.unified_campaign_name,'-',4),'_',1)
                                                            else split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                                       end
                                                  )
                                             end
                                       )
                                       when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when upper(source) = 'APPLE SEARCH ADS' then (
                                             case
                                                  when x1.unified_campaign_name like 'GP\\\_%' then split_part(x1.unified_campaign_name,'_',8)
                                                  when x1.unified_campaign_name like 'adquant\\\_GP\\\_%' then split_part(x1.unified_campaign_name,'_',9)
                                                  else split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                             end
                                       )
                                       when upper(source) = 'DIGITAL TURBINE' then (
                                             case
                                                  when x1.unified_campaign_name like 'TubiTV-Android-%' then split_part(x1.unified_campaign_name,'-',4)
                                                  when x1.unified_campaign_name like 'TubiTV-%-Android-%' then split_part(x1.unified_campaign_name,'-',5)
                                             end
                                       )
                                       when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                             case
                                                  when x1.unified_campaign_name similar to '[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*-[a-zA-Z0-9\\\s]*\\\_%' then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                                  when x1.unified_campaign_name like 'US\\_%' or x1.unified_campaign_name like '\\\_\\\_US\\\_%' then 'US'
                                             end
                                       )
                                       when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                       when upper(source) = 'LINEAR TV' then (
                                             case
                                                  when DATE(DATE) < '2023-08-01' then split_part(x1.unified_campaign_name,'-',3)
                                                  else split_part(split_part(x1.unified_campaign_name,'-',3),'_',1)
                                             end
                                       
                                       )
                                       when upper(source) = 'MOLOCO' THEN split_part(split_part(unified_campaign_name,'-',3),'_',1)
                                   end) as campaign_Country,
                              case
                                   when team_spend = 'BD SPEND' then 'BD'
                                   when team_spend = 'ENGINEERING SPEND' then 'ROT'
                                   when team_spend = 'GROWTH SPEND' then (
                                        case
                                             when upper(source) = 'AMAZON MEDIA GROUP' then (
                                                  case
                                                       when DATE(DATE) >= '2023-08-01' and unified_campaign_id not in ('591896114360588456','584081239010065198','589413725812369512','590148632198350252') and regexp_count(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',8),'-')>=1 then split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',8),'-',1)
                                                       when x1.unified_campaign_name not like '%Growth Rotational%' then (
                                                            case
                                                                 when x1.unified_campaign_name like '%SOV-25%' then 'SOV25'
                                                                 when x1.unified_campaign_name like '%SOV-50%' then 'SOV50'
                                                                 when x1.unified_campaign_name like '%SOV-75%' then 'SOV75'
                                                                 when x1.unified_campaign_name like '%SOV-100%' then 'SOV100'
                                                                 when x1.unified_campaign_name like '%25\\\% SOV%' then 'SOV25'
                                                                 when x1.unified_campaign_name like '%50\\\% SOV%' then 'SOV50'
                                                                 when x1.unified_campaign_name like '%75\\\% SOV%' then 'SOV75'
                                                                 when x1.unified_campaign_name like '%100\\\% SOV%' then 'SOV100'
                                                                 when x1.unified_campaign_name like '%SOV%' and x1.unified_campaign_name like '%AMZN US Fire TV Live Tab SOV%' and ds>='2022-01-01' then 'SOV100'
                                                                 when x1.unified_campaign_name like '%SOV%' then 'SOV'
                                                                 when x1.unified_campaign_name ilike '%takeover%' then 'RB'
                                                                 when x1.unified_campaign_name ilike '%roadblock%' then 'RB'
                                                                 else 'ROT'
                                                            end
                                                       )
                                                       else 'ROT'
                                                  end
                                             )
                                             when platform_v2='ROKU' then (
                                                  case
                                                       when DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',9),'-',1)
                                                       when DATE(date) >= '2023-10-01' then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',8),'-',1)
                                                       when upper(x1.unified_campaign_name) like '%SPOTLIGHT%' or x1.unified_campaign_name like '%\\\_Spotlight Ad%' then 'SOV100'
                                                       when x1.unified_campaign_name like '%Channel Store - Featured Row%' then 'RB'
                                                       when (x1.unified_campaign_name like '%ChannelStoreR%' and x1.unified_campaign_name like '%FlatRate%') then 'RB'
                                                       when (x1.unified_campaign_name like '%FeaturedFreeR%' and x1.unified_campaign_name like '%FlatRate%') then 'RB'
                                                       when x1.unified_campaign_name like '%Featured Free – Themed Row%' then 'RB'
                                                       when x1.unified_campaign_name like '%RON Weekday Display Takeover (6-7pm UTZ)%' then 'RB'
                                                       when x1.unified_campaign_name like '%15SOV,%' then 'SOV15'
                                                       when x1.unified_campaign_name like '%SOV100%' then 'SOV100'
                                                       when x1.unified_campaign_name like '%HomeScreenSOV0915%' then 'SOV100'
                                                       when x1.unified_campaign_name like '%SOV,%' then 'SOV'
                                                       else 'ROT'
                                                  end
                                             )
                                             when upper(source) = 'SAMSUNG' then (
                                                  case
                                                       when DATE(DATE) >= '2023-09-01' and right(trim(unified_campaign_name), 5) != 'aug23' then split_part(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',8),'-',1)
                                                       when x1.unified_campaign_name like '%\\\_RB-%' or x1.unified_campaign_name ilike '%Takeover%' then 'RB'
                                                       else 'ROT'
                                                  end
                                             )
                                             when upper(source) = 'VIZIO' then (
                                                  case
                                                       when DATE(date) >= '2023-09-01' then split_part(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',8),'-',1)
                                                       when (
                                                                 (x1.unified_campaign_name like '%Homescreen Custom Carousel%' and ds<'2023-01-01')
                                                            or
                                                                 (x1.unified_campaign_name like '%\\\_SOV-100%')
                                                            or
                                                                 (x1.unified_campaign_name like '%TKO%')
                                                            or
                                                                 (x1.unified_campaign_id='1130769')
                                                       ) then 'RB'
                                                       else 'ROT'
                                                  end
                                             )
                                             when upper(source) = 'LG ADS' then (
                                                  case
                                                       when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',1)
                                                       when (
                                                                 x1.unified_campaign_name like '%Roadblock%' 
                                                            or
                                                                 x1.unified_campaign_name like '%Takeover%' 
                                                            or
                                                                 -- Will need to update this once Sponsored Search converted into a bidding product
                                                                 x1.unified_campaign_name like '%\\\_FlatFee\\\_SponsoredSearch\\\_%'
                                                            or
                                                                 -- Will need to update this once Sponsored Search converted into a bidding product
                                                                 x1.unified_campaign_name like '%\\\_FlatRate\\\_SponsoredSearch\\\_%'
                                                       ) then 'RB'
                                                       else 'ROT'
                                                  end
                                             )
                                             when upper(source) = 'XBOX' then (
                                                  case
                                                       when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',1)
                                                       when x1.unified_campaign_name like '%Roadblock%' then 'RB'
                                                       else 'ROT'
                                                  end
                                             )
                                             when upper(source) in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 and DATE(DATE) >= '2023-08-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',1)
                                             when upper(source) = 'MOLOCO' THEN split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',1)
                                             else 'ROT'   
                                             end         
                                   )
                              end as campaign_type,
                              case
                                   when team_spend = 'BD SPEND' then (
                                        case
                                             when DATE(DATE) >= '2023-08-01' then (
                                                  case
                                                       when upper(source) = 'VIZIO' then split_part(split_part(split_part(campaign_name,regexp_substr(campaign_name, '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2),'-',1)
                                                       when upper(source) = 'SAMSUNG' then split_part(split_part(split_part(unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',2),'-',1)
                                                       else split_part(split_part(unified_campaign_name,'_',2),'-',1)
                                                  end
                                             )
                                             when unified_campaign_name like '%OOBE%' then 'NEW'
                                             else 'ALL'
                                        end
                                   )
                                   when team_spend = 'ENGINEERING SPEND' then 'ALL'
                                   when team_spend = 'GROWTH SPEND' then (
                                        case
                                             when trim(campaign_type) <> 'ROT' then 'ALL'
                                             -- Might need to update this once we fixed the platform value for FOX LOCAL SPEND
                                             when source='Linear TV' and x1.unified_campaign_name ilike '%Fox%Local%' then 'ALL'
                                             when source='Direct Mail' then 'ALL'
                                             when source='Valnet' and DATE(DATE) >= '2023-07-01' and DATE(DATE) <= '2023-07-31' then 'ALL' 
                                             when platform_v2='YOUTUBE CTV' then (
                                                  case
                                                       when x1.unified_campaign_name like '%\\\_RMKT\\\_%' then 'RTG'
                                                       when x1.unified_campaign_name like '%CTV%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when source in ('AdWords','Facebook','Google Marketing Platform (DV360)','theTradeDesk','RevX') then (
                                                  case 
                                                       when source='Facebook' and x1.unified_campaign_name ilike 'Avatar%' then 'ALL'
                                                       when (x1.unified_campaign_name ilike '%lapse%' or x1.unified_campaign_name ilike '%retarget%' or x1.unified_campaign_name like '%\\\_RMKT\\\_%') then 'RTG'
                                                       when x1.unified_campaign_name ilike '%\\\_Google\\\_UAC\\\_ACE\\\_Event\\\_ViewItem\\\_%' then 'RTG'
                                                       when x1.unified_campaign_name like '%Google%' and x1.unified_campaign_name like '%Search%' and (x1.unified_campaign_name like '%Desktop%' or x1.unified_campaign_name like '%Web%' or x1.unified_campaign_name like '%Mobile%') then 'ALL'
                                                       when x1.unified_campaign_name like '%Google\\\_Desktop\\\_Display%' then 'ALL'
                                                       when x1.unified_campaign_name like '%Google\\\_Desktop\\\_DSA%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when upper(source) = 'AMAZON MEDIA GROUP' then (
                                                  case 
                                                       when DATE(DATE) >= '2023-08-01' and unified_campaign_id not in ('591896114360588456','584081239010065198','589413725812369512','590148632198350252') and regexp_count(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',2),'-')>=1 then split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',2),'-',1)
                                                       when x1.unified_campaign_name ilike '%\\\_lapse%' then 'RTG'
                                                       when x1.unified_campaign_name ilike '%lapse\\\_%' then 'RTG'
                                                       when x1.unified_campaign_name ilike '%retarget%' then 'RTG'
                                                       when x1.unified_campaign_name ilike '%- Retention -%' then 'RTG'
                                                       else 'NEW'
                                                  end
                                             )
                                             when platform_v2='ROKU' then (
                                                  case 
                                                       when DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',3),'-',1)
                                                       when DATE(date) >= '2023-10-01' then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',2),'-',1)
                                                       when (x1.unified_campaign_name like '%Reactivation%') or (x1.unified_campaign_name ilike '%lapsed%') or (x1.unified_campaign_name ilike '%retarget%') or (x1.unified_campaign_name ilike '%liveramp%') or (x1.unified_campaign_name like '%S3%') then 'RTG'
                                                       -- Double check this?
                                                       when x1.unified_campaign_name like '%Native Display, D7-D59 + %(Winback)%' then 'RTG'
                                                       when x1.unified_campaign_name like '%TRC%' and x1.unified_campaign_name ilike '%video%' then 'ALL'
                                                       when x1.unified_campaign_name like '%Featured Free Tile%' and x1.unified_campaign_name ilike '%featuredfree%' then 'ALL'
                                                       -- Double check this?
                                                       when x1.unified_campaign_name like '%\\\_Video -%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when upper(source) = 'SAMSUNG' then (
                                                  case 
                                                       when DATE(DATE) >= '2023-09-01' and right(trim(unified_campaign_name), 5) != 'aug23' then split_part(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',2),'-',1)
                                                       when (x1.unified_campaign_name ilike '%lapse%') or (x1.unified_campaign_name ilike '%Opened-0Watch%') or (x1.unified_campaign_name ilike '%retarget%') or (x1.unified_campaign_name ilike '%liveramp%') or (x1.unified_campaign_name like '%\\\_S3%') then 'RTG'
                                                       when x1.unified_campaign_name like '%Untarget%' then 'ALL'
                                                       when x1.unified_campaign_name like '% UTG %' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when upper(source) = 'VIZIO' then (
                                                  case 
                                                       when DATE(date) >= '2023-09-01' then split_part(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2),'-',1)
                                                       when (x1.unified_campaign_name ilike '%\\\_targeting\\\_%') or (x1.unified_campaign_name ilike '%lapse%') or (x1.unified_campaign_name ilike '%Return-Users%') or (x1.unified_campaign_name like '%\\\_S3%') then 'RTG'
                                                       when x1.unified_campaign_name ilike '%untargeted%' then (
                                                            case
                                                            when x1.unified_campaign_name like '%\\\_New\\\_Users\\\_%' then 'NEW'
                                                            when x1.unified_campaign_name like '%\\\_Lapsed\\\_Users\\\_%' then 'RTG'
                                                            else 'ALL'
                                                            end
                                                       )
                                                       when ds<'2022-04-01' and x1.unified_campaign_name not like 'VIZIO_Tubi%_Suppress%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when upper(source) = 'LG ADS' then (
                                                  case 
                                                       when DATE(date) >= '2023-09-01' then  split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',1)
                                                       when x1.unified_campaign_name like '%\\\_FlatFee\\\_SponsoredSearch\\\_%' then 'ALL'
                                                       when x1.unified_campaign_name like '%\\\_FlatRate\\\_SponsoredSearch\\\_%' then 'ALL'
                                                       when x1.unified_campaign_name like '%NonTubiInstall%' then 'NEW'
                                                       when (x1.unified_campaign_name ilike '%lapse%') or (x1.unified_campaign_name ilike '%:\\\_Tubi\\\_Installs%') or (x1.unified_campaign_name ilike '%\\\_TubiInstalls\\\_%') or (x1.unified_campaign_name ilike '%-TubiInstalls\\\_%') or (x1.unified_campaign_name ilike '%- Soccer Fan Installed -%') or (x1.unified_campaign_name ilike '%- Tubi Installs%') or (x1.unified_campaign_name ilike '%- Fans of Soccer w/ Tubi%') then 'RTG'
                                                       when x1.unified_campaign_name like '%LG Ads - Tubi - Retargeting%' then 'RTG'
                                                       when x1.unified_campaign_name like '%\\\_-\\\_Tubi\\\_Installs-Display%' then 'RTG'
                                                       when x1.unified_campaign_name like '%\\\_-\\\_Tubi\\\_Install-Display%' then 'RTG'
                                                       when x1.unified_campaign_name like '%\\\_AddedValue\\\_%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when upper(source) = 'XBOX' then (
                                                  case 
                                                       when DATE(date) >= '2023-09-01' then  split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',1)
                                                       when (x1.unified_campaign_name ilike '%-TubiOwners%' or x1.unified_campaign_name ilike '%Lapse%' or x1.unified_campaign_name like '%Re-EngagementAudienceExcludingSuperDevices%' or x1.unified_campaign_name like '%\\\_Reengagement\\\_%') then 'RTG'
                                                       when x1.unified_campaign_name ilike '%untarget%' then 'ALL'
                                                       else 'NEW'
                                                  end
                                             )
                                             when platform_v2 in ('PS4','PS5','COMCAST') then split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',1)
                                             -- Need to update logic for PlayStation, AndroidTV, Youtube CTV once we have non untargeted campaigns
                                             when platform_v2='PLAYSTATION' then 'ALL' 
                                             when platform_v2='ANDROIDTV' then 'ALL'
                                             when x1.unified_campaign_name ilike '%retargeting%' then 'RTG'
                                             when upper(source) = 'MOLOCO' THEN split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',1)
                                             else 'NEW'
                                        end
                                   )
                              end as campaign_targeting_group,
                            case when team_spend='BD SPEND' and DATE(DATE) >= '2023-08-01' then (
                                   case
                                        when platform_v2='VIZIO' then trim(substring(split_part(upper(split_part(unified_campaign_name,regexp_substr(unified_campaign_name, '([0-9]{4})(-|_)([0-9]{4})(_)'),2)),'_',2),len(split_part(split_part(split_part(unified_campaign_name,regexp_substr(unified_campaign_name, '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2),'-',1))+2))
                                        else trim(substring(split_part(upper(unified_campaign_name),'_',2),len(split_part(split_part(unified_campaign_name,'_',2),'-',1))+2))
                                   end
                            
                                 ) 
                                 when platform_V2 = 'ROKU' then 
                                   case when DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',3),'-',2)
                                        when DATE(date) >= '2023-10-01' then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',2),'-',2)
                                        when DATE(DATE) >= '2022-07-05' then case when DATE(DATE) >= '2023-08-01' and x1.unified_campaign_id='1002001001' then trim(substring(split_part(trim(regexp_substr(upper(unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p')),',',2),len(split_part(split_part(trim(regexp_substr(upper(unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p')),',',2),'-',1))+2))
                                                                                                               when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),'-',3),',',2)
                                                                                                               when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),'-',3),',',2)
                                                                                                          end
                                                                else case when lower(trim(x1.unified_campaign_name)) = 'ics' then 'ICS'
                                                                          when upper(x1.unified_campaign_name) like '%RON%' or upper(x1.unified_campaign_name) like '%MAKEGOOD%' then 'RON'
                                                                          when upper(x1.unified_campaign_name) like '%AVOD CONQUESTING%' then 'AVOD CONQUESTING'
                                                                          when upper(x1.unified_campaign_name) like '%REACTIVATION D7-D59%' then 'REACTIVATION D7-D59'
                                                                          when upper(x1.unified_campaign_name) like '%REACTIVATION D60-D90%' or upper(x1.unified_campaign_name) like '%REACTIVATION: D60-D90%' or upper(x1.unified_campaign_name) like '%D60-D90 LAPSED%' then 'REACTIVATION D60-D90'
                                                                          when upper(x1.unified_campaign_name) like '%REACTIVATION D90+%' or upper(x1.unified_campaign_name) like '%REACTIVATION: D90+%' or upper(x1.unified_campaign_name) like '%D90+ LAPSED%'  then 'REACTIVATION D90+'
                                                                          when upper(x1.unified_campaign_name) like '%FTV PREDICTIVE MODEL%' then 'FTV PREDICTIVE MODEL'
                                                                          when upper(x1.unified_campaign_name) similar to '%NEW ROKU USERS%|%NEW USERS%' then 'NEW USERS'
                                                                          when upper(x1.unified_campaign_name) like '%GAMING%' then 'GAMING'
                                                                          when upper(x1.unified_campaign_name) like '%FIRST TIME QSS%' then 'FIRST TIME QSS'
                                                                          when upper(x1.unified_campaign_name) like '%DMP AUDIENCE%' then 'DMP AUDIENCE'
                                                                          when upper(x1.unified_campaign_name) like '%SPANISH DEVICES%' then 'SPANISH DEVICES'
                                                                          when upper(x1.unified_campaign_name) like '%D7-D59 + ACTION%' then 'D7-D59 + ACTION'
                                                                          when upper(x1.unified_campaign_name) like '%D7-D59 + AA COMEDY%' then 'D7-D59 + AA COMEDY'
                                                                          when upper(x1.unified_campaign_name) like '%D7-D59 + COMEDY%' then 'D7-D59 + COMEDY'
                                                                          when upper(x1.unified_campaign_name) like '%COMEDY%' then 'COMEDY'
                                                                          when upper(x1.unified_campaign_name) like '%ACTION%' then 'ACTION'
                                                                          when upper(x1.unified_campaign_name) like '%HEAVY NEWS%' then 'HEAVY NEWS'
                                                                          when upper(x1.unified_campaign_name) like '%BALTIMORE%' then 'BALTIMORE'
                                                                    end
                                                                end

                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when DATE(DATE) >= '2023-08-01' and unified_campaign_id not in ('591896114360588456','584081239010065198','589413725812369512','590148632198350252') then trim(substring(split_part(trim(split_part(upper(unified_campaign_name),'|',2)),'_',2),len(split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',2),'-',1))+2))
                                                                                                                    when unified_campaign_name like '%\\\_ROT-%' then trim(substring(split_part(trim(split_part(upper(unified_campaign_name),'|',2)),'_',2),len(split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',2),'-',1))+2))
                                                                                                                    when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',2)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',2) + '-' + split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',3)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',2)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',2)
                                                                                                               END
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when upper(x1.unified_campaign_name) similar to '%UNTARGETED%|%SOV%' then 'RON'
                                                                                     when position ('|' in x1.unified_campaign_name) > 0 then split_part(split_part(x1.unified_campaign_name,'|',2),'_',2)
                                                                                     when upper(x1.unified_campaign_name) like '%NULL%' then split_part(regexp_substr(upper(x1.unified_campaign_name),'NULL[^.]*',1,1,'i') ,'_',2)
                                                                                     when upper(x1.unified_campaign_name) similar to '%OOBE%|%OUT OF BOX EXPERIENCE%' then 'NEW USERS'
                                                                                     when (upper(x1.unified_campaign_name) like '%ROTATIONAL%' and upper(x1.unified_campaign_name) similar to '%SPANISH%|%SPAN%|%CONTENT FORWARD ST%') or upper(x1.unified_campaign_name) like '%CONTENT FORWARD ST%'  then 'SPANISH DEVICES'
                                                                                     when upper(x1.unified_campaign_name) similar to '%ROTATIONAL%|%GUARANTEED%' and upper(x1.unified_campaign_name) not similar to '%SPANISH%|%SPAN%|%CONTENT FORWARD ST%' and upper(x1.unified_campaign_name) not similar to '%TARGETED%' then 'RON'
                                                                                     when upper(x1.unified_campaign_name) similar to '%TARGETED%' then 'TARGETED' -- THIS SHOULD ALWAYS BE LAST AND IS A CATCH FOR TARGETED
                                                                                end
                                 when upper(source) = 'VIZIO' and DATE(date) >= '2023-09-01' then SUBSTRING(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2),position('-' in split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2))+1)
                                 when upper(source) = 'VIZIO' then case when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%CFORWARD' then 'UNTARGETED-BTF'
                                                                                                                                                                when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',2)
                                                                                                                                                           end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',2)
                                                                        when upper(x1.unified_campaign_name) like '%TARGET%' or upper(x1.creative_name) like '%TARGET%' then 'TARGETING' --Have opened the app before, returning and lapsed users
                                                                        when upper(x1.unified_campaign_name) like '%SUPPRESS%' or upper(x1.creative_name) like '%SUPPRESS%' then 'SUPPRESS' --Targets New Users Only
                                                                        when upper(x1.unified_campaign_name) not like '%TARGET%' and upper(x1.unified_campaign_name) not like '%SUPPRESS%' then 'UNTARGETED'
                                                                   end
                                 when upper(source) = 'SAMSUNG' then case when DATE(DATE) >= '2023-09-01' and right(trim(unified_campaign_name), 5) != 'aug23' then SUBSTRING(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',2),position('-' in split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',2))+1)
                                                                          when DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',2)
                                                                          when upper(x1.unified_campaign_name) like '%LAPSED USER%' and upper(x1.unified_campaign_name) not like '%AND LAPSED USER%'  then 'LAPSED USER'
                                                                          when upper(x1.unified_campaign_name) like '%AND%' then REGEXP_SUBSTR(upper(x1.unified_campaign_name),'[^-|\|]*AND[^-|\|]*')
                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 3 then split_part(upper(x1.unified_campaign_name),'|',3)
                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 2 then split_part(upper(x1.unified_campaign_name),'|',2)
                                                                          when upper(x1.unified_campaign_name) similar to '%UTG%|%FIRST SCREEN%' then 'UNTARGETED'
                                                                          when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then split_part(upper(x1.unified_campaign_name),' - ',2)
                                                                     end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' then SUBSTRING(split_part(upper(x1.unified_campaign_name),'_',2),position('-' in split_part(upper(x1.unified_campaign_name),'_',2))+1)
                                                                         when DATE(date) >= '2022-10-13' and unified_campaign_id='1005005001' then 'EVERYONE'
                                                                         when DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',2)
                                                                         when upper(x1.unified_campaign_name) like '%ROADBLOCK%' then 'UNTARGETED'
                                                                         when regexp_count(x1.unified_campaign_name, ':') = 1 then replace(split_part(split_part(upper(x1.unified_campaign_name),':',2),'-DISPLAY',1),'_',' ')
                                                                         when upper(x1.unified_campaign_name) like '%UNTARGETED%' then 'UNTARGETED'
                                                                         when regexp_count(x1.unified_campaign_name, '\\\_\\\-\\\_') = 2 then replace(split_part(split_part(upper(x1.unified_campaign_name),'_-_',3),'-DISPLAY',1),'_',' ')
                                                                    end
                               when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',2)
                                                                     when DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',2)
                                                                     when upper(x1.unified_campaign_name) like '%ROADBLOCK%' then 'UNTARGETED'
                                                                     when regexp_count(replace(x1.unified_campaign_name,'(','_-_'), '\\\_\\\-\\\_') = 1 then split_part(upper(x1.unified_campaign_name),'(',1) -- had to add in the replace because Data Bricks was not liking regex searching for a parenthesis
                                                                     when regexp_count(x1.unified_campaign_name, '\\\_') = 8 then split_part(upper(x1.unified_campaign_name),'_',2)
                                                                end
                               when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'ADWORDS' then (
                                    case
                                        when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',2)
                                        when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then split_part(x1.unified_campaign_name,'_',2)
                                        end
                               )
                               when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                   case
                                        when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',4)
                                        else split_part(x1.unified_campaign_name,'_',2)
                                   end
                               )
                               when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',2)
                               when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then case when DATE(DATE) >= '2023-08-01' then trim(substring(split_part(upper(unified_campaign_name),'_',2),len(split_part(split_part(unified_campaign_name,'_',2),'-',1))+2))
                                                                                                                                             else split_part(upper(unified_campaign_name),'_',2)
                                                                                                                                        end
                               when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then substring(split_part(x1.unified_campaign_name, '_', 2),regexp_instr(split_part(x1.unified_campaign_name, '_', 2),'-')+1)
                               when upper(source) = 'MOLOCO' THEN split_part(split_part(upper(x1.unified_campaign_name),'_',2),'-',2)
                            end as campaign_targeting,
                            case when platform_V2 = 'ROKU' THEN
                              CASE WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(x1.unified_campaign_name, '_',4) 
                                   WHEN DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',3)
                                                                                                    when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',3)
                                                                                                end
                                   END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',3)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',4)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',3)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',3)
                                                                                                               END
                                 when upper(source) = 'XBOX' then case when DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',3)
                                                                  end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2022-10-13' and DATE(date) < '2023-09-01' and unified_campaign_id='1005005001' then 'FLATRATE'
                                                                         when DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',3)
                                                                    end
                                 when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',3)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',3)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',3) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',3)
                                                                   end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',3)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',4)
                                                       else split_part(x1.unified_campaign_name,'_',3)
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',5)
                                             else split_part(x1.unified_campaign_name,'_',3)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',3)
                                 when upper(source) = 'MOLOCO' THEN split_part(x1.unified_campaign_name,'_',3)
                            end as campaign_Bid_type,
                             case when team_spend='BD SPEND' and DATE(DATE) >= '2023-08-01' then (
                                   case
                                        when platform_v2='VIZIO' then split_part(split_part(upper(unified_campaign_name),regexp_substr(upper(unified_campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',4)
                                        else split_part(upper(x1.unified_campaign_name),'_',4)
                                   end
                            
                                 )
                                 when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',4)
                                                                        when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',5)
                                                                        when DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',4)
                                                                                                                  when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',4)
                                                                                                             end
                                                                        when lower(trim(x1.unified_campaign_name)) = 'ics' then 'ICS'
                                                                        when upper(x1.unified_campaign_name) like '%BALTIMORE%' or upper(x1.creative_name) like '%30SEC%' then 'LINEAR VIDEO'
                                                                        when upper(x1.creative_name) like '%525X735%' then 'HOME SCREEN'
                                                                        when upper(x1.creative_name) like '%406X131%' or upper(x1.creative_name) like '%186X261%' then 'SCREENSAVER'
                                                                        when upper(x1.unified_campaign_name) like '%NATIVE DISPLAY%' or upper(x1.unified_campaign_name) like '%MAKEGOOD%' or upper(x1.unified_campaign_name) like '%M&E - GUARANTEED%' then 'NATIVE DISPLAY'
                                                                        when upper(x1.creative_name) like '%600X500%' then 'MOBILE REMOTE'
                                                                   end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',5)
                                                                                     when DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',4)
                                                                                                                               when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',5)
                                                                                                                                                                                                 else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',4)
                                                                                                                                                                                            end
                                                                                                                               when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',4)
                                                                                                                          END
                                                                                     when upper(x1.unified_campaign_name) like '%LIVE%' then 'LIVE TAB'
                                                                                     when upper(x1.unified_campaign_name) similar to '%SS%|%SCREENSAVER%' then 'SCREENSAVER'
                                                                                     when upper(x1.unified_campaign_name) similar to '%HOME%|%TILE%' and upper(x1.unified_campaign_name) not similar to '%ATF%|%BTF%'  then 'HOME'
                                                                                     when upper(x1.unified_campaign_name) like '%ATF%' then 'HOME ATF'
                                                                                     when upper(x1.unified_campaign_name) like '%BTF%' then 'HOME BTF'
                                                                                     when upper(x1.unified_campaign_name) similar to '%OOBE%|%OUT OF BOX EXPERIENCE%' then 'ONBOARDING'
                                                                                end

                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',5)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%CFORWARD' then 'CAROUSEL'
                                                                                                                                                                when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',4)
                                                                                                                                                           end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',4)
                                                                        when upper(x1.creative_name) similar to ('%H1-5%|%H1-6%|%H1-H6%') or upper(x1.unified_campaign_name) similar to ('%H1-5%|%H1-6%|%H1-H6%') then 'HERO'
                                                                        when upper(x1.creative_name) similar to ('%D1-4%|%D1-6%|%D1-5%') or upper(x1.unified_campaign_name) similar to ('%D1-4%|%D1-6%|%D1-5%') then 'DISCOVER'
                                                                   end

                                 when upper(source) = 'SAMSUNG' then case when DATE(DATE) >= '2023-09-01' AND trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',5)
                                                                          when DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',4)
                                                                          when upper(x1.creative_name) like '%UGM%' then 'Universal Guide - Mast Head'
                                                                          when upper(x1.creative_name) like '%ASM%' then 'App Store - Mast Head'
                                                                          when upper(x1.creative_name) like '%FS%' then 'First Screen'
                                                                     end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',5)
                                                                       when DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',4)
                                                                       when regexp_count(x1.unified_campaign_name, '\\\_') = 8 then split_part(upper(x1.unified_campaign_name),'_',4)
                                                                  end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',5)
                                                                         when DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',4)
                                                                    end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',4)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',5)
                                                       else split_part(x1.unified_campaign_name,'_',4)
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',6)
                                             else split_part(x1.unified_campaign_name,'_',4)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when platform_V2 in ('PS4','PS5','COMCAST') then split_part(creative_name,'_',5)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',4)
                                 when upper(source) = 'MOLOCO' THEN split_part(x1.creative_name,'_',5)
                            end as campaign_ad_placement,
                            case when platform_V2 = 'ROKU' THEN
                              CASE WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(x1.unified_campaign_name, '_',6) 
                                   WHEN DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',5)
                                                                                                      when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',5)
                                                                                                 end
                                   END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',5)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',6)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',5)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',5)
                                                                                                               END
                                 when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',5)
                                 when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',5)
                                 when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',5)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',5)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',5) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',5)
                                                                   end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',5)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',6)
                                                       else split_part(x1.unified_campaign_name,'_',5)
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',7)
                                             else split_part(x1.unified_campaign_name,'_',5)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',5)
                                 when upper(source) = 'MOLOCO' THEN split_part(x1.unified_campaign_name,'_',5)
                            end as campaign_Device,
                            case when platform_V2 = 'ROKU' THEN
                              CASE WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(x1.unified_campaign_name, '_',7) 
                                   WHEN DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',6)
                                                                                                      when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',6)
                                                                                                 end
                                   END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',6)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',7)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',6)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',6)
                                                                                                               END
                                 when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',6)
                                 when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',6)
                                 when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',6)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',6)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',6) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',6)
                                                                   end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',6)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',7)
                                                       else split_part(x1.unified_campaign_name,'_',6)
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',8)
                                             else split_part(x1.unified_campaign_name,'_',6)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',6)
                                 when upper(source) = 'MOLOCO' then split_part(x1.unified_campaign_name,'_',6)
                            end as campaign_Platform,
                            case when platform_V2 = 'ROKU' THEN
                              CASE WHEN DATE(DATE) >= '2024-12-31' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(x1.unified_campaign_name, '_',8) 
                                   WHEN DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',7)
                                                                                                      when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',7)
                                                                                                 end
                                   END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',7)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',8)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',7)
                                                                                                                                                                              end
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',7)
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',7)
                                                                                                               END
                                 when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',7)
                                 when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',7)
                                 when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',7)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',7)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',7) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',7)
                                                                   end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',7)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',8)
                                                       else split_part(x1.unified_campaign_name,'_',7)
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',9)
                                             else split_part(x1.unified_campaign_name,'_',7)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',7)
                                 when upper(source) = 'MOLOCO' then split_part(x1.unified_campaign_name,'_',7)
                            end as campaign_Publisher,
                            case when team_spend = 'BD SPEND' and DATE(DATE) >= '2023-08-01' then (
                                   case
                                        when platform_v2='VIZIO' then trim(substring(split_part(upper(split_part(unified_campaign_name,regexp_substr(unified_campaign_name, '([0-9]{4})(-|_)([0-9]{4})(_)'),2)),'_',8),len(split_part(split_part(split_part(unified_campaign_name,regexp_substr(unified_campaign_name, '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',8),'-',1))+2))
                                        else trim(substring(split_part(upper(unified_campaign_name),'_',8),len(split_part(split_part(unified_campaign_name,'_',8),'-',1))+2))
                                   end
                            
                                 ) 
                                 when platform_V2 = 'ROKU' THEN
                                 CASE WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(split_part(x1.unified_campaign_name, '_',9),'-',2) 
                                        WHEN DATE(DATE) >= '2022-07-05' then case when DATE(DATE) >= '2023-08-01' and x1.unified_campaign_id='1002001001' then trim(substring(split_part(trim(regexp_substr(upper(unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p')),',',8),len(split_part(split_part(trim(regexp_substr(upper(unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p')),',',8),'-',1))+2))
                                                                                                    when DATE(DATE) >= '2023-10-01' then split_part(split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',8),'-',2)
                                                                                                    when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',8)
                                                                                                    when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',8)
                                                                                                end
                                        END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when DATE(DATE) >= '2023-08-01' and unified_campaign_id not in ('591896114360588456','584081239010065198','589413725812369512','590148632198350252') then trim(substring(split_part(trim(split_part(upper(unified_campaign_name),'|',2)),'_',8),len(split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',8),'-',1))+2))
                                                                                                                    when unified_campaign_name like '%\\_ROT-%' then trim(substring(split_part(trim(split_part(upper(unified_campaign_name),'|',2)),'_',8),len(split_part(split_part(trim(split_part(unified_campaign_name,'|',2)),'_',8),'-',1))+2))
                                                                                                                    when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',8)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',9)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',8)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',8)
                                                                                                               END
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',2)
                                                                       when DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',8)
                                                                  end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',2)
                                                                         when DATE(date) >= '2022-10-13' and unified_campaign_id='1005005001' then 'NG'
                                                                         when DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',8)
                                                                    end
                                 when upper(source) = 'SAMSUNG' then case when DATE(date) >= '2023-09-01' and right(trim(unified_campaign_name), 5) != 'aug23' then split_part(split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',8),'-',2)
                                                                          when DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',8)
                                                                     end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',8),'-',2)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',8) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',8)
                                                                   end
                                 when upper(source) = 'ADWORDS' then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',8)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' and upper(x1.unified_campaign_name) similar to '%SEARCH%(DESKTOP|MOBILE|WEB)%' then split_part(x1.unified_campaign_name,'_',8)
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then case when DATE(DATE) >= '2023-08-01' then trim(substring(split_part(upper(unified_campaign_name),'_',8),len(split_part(split_part(unified_campaign_name,'_',8),'-',1))+2))
                                                                                                                                               else split_part(upper(unified_campaign_name),'_',8)
                                                                                                                                          end
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then substring(split_part(x1.unified_campaign_name, '_', 8),regexp_instr(split_part(x1.unified_campaign_name, '_', 8),'-')+1)
                                 when upper(source) = 'MOLOCO' THEN split_part(split_part(upper(x1.unified_campaign_name),'_',8),'-',2)
                            end as campaign_Delivery,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                                 when upper(source) = 'ADWORDS' and platform_V2 <> 'YOUTUBE CTV' and DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' and upper(x1.unified_campaign_name) not similar to '%SEARCH%(DESKTOP|MOBILE|WEB)%' then (
                                   case
                                        when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then split_part(x1.unified_campaign_name,'_',9)
                                        else split_part(x1.unified_campaign_name,'_',8)
                                   end
                                 )
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then split_part(x1.unified_campaign_name,'_',8)
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 7 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',10)
                                             when split_part(x1.unified_campaign_name,'_',8) like 'ROT-%' then substring(split_part(x1.unified_campaign_name,'_',8),5)
                                             else split_part(x1.unified_campaign_name,'_',8)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',8)
                            end as campaign_Optimization_Event,
                            case when platform_V2 = 'ROKU' THEN
                                   CASE WHEN DATE(DATE) >= '2025-01-01' AND x1.unified_campaign_name NOT LIKE '%,%' THEN split_part(x1.unified_campaign_name, '_',10) 
                                        WHEN DATE(DATE) >= '2022-07-05' then case when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,1,'p'),',',9)
                                                                                                      when regexp_count(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',') >= 8 then split_part(regexp_substr(upper(x1.unified_campaign_name), '(?<=\\\().*?(?=\\\))',1,2,'p'),',',9)
                                                                                                 end
                                   END
                                 when upper(source) = 'AMAZON MEDIA GROUP' AND DATE(DATE) >= '2022-05-01' THEN CASE when upper(x1.unified_campaign_name) like '%SCREENSAVER:%' then split_part(split_part(upper(x1.unified_campaign_name),'SCREENSAVER:',2),'_',9)
                                                                                                                    when regexp_count(x1.unified_campaign_name, '\\\|') = 1 then case when upper(x1.unified_campaign_name) similar to '%TARGETED\\\_BTF%|%TARGETED\\\_ATF%' then split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',10)
                                                                                                                                                                                   else split_part(split_part(upper(x1.unified_campaign_name),'|',2),'_',9)
                                                                                                                                                                              end
                                                                                                                    when upper(x1.unified_campaign_name) like '%ROTATIONAL - %' then split_part(split_part(upper(x1.unified_campaign_name),'ROTATIONAL - ',2),'_',9)
                                                                                                               END
                                 when upper(source) = 'XBOX' AND DATE(DATE) >= '2022-05-01' THEN split_part(upper(x1.unified_campaign_name),'_',9)
                                 when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-06-03' AND DATE(DATE) != '2022-06-07' THEN split_part(upper(x1.unified_campaign_name),'_',9)
                                 when upper(source) = 'SAMSUNG' AND DATE(DATE) >= '2022-06-01' AND regexp_count(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'\\\_') >= 7 then split_part(split_part(x1.unified_campaign_name,'|',(regexp_count(x1.unified_campaign_name,'\\\|')+1)),'_',9)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',9)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name),'--US'),2),'_',9) end
                                                                        when DATE(DATE) >= '2022-05-18' then split_part(split_part(upper(campaign_name),regexp_substr(upper(campaign_name), '([0-9]{4})(-|_)([0-9]{4})(_){1,}'),2),'_',9)
                                                                   end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-09-01' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'ADWORDS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then (
                                        case
                                             when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.unified_campaign_name,'_',9)
                                             when DATE(DATE) >= '2023-01-22' and x1.unified_campaign_name not like 'GP\\\_%' and x1.unified_campaign_name not like 'zzzGP\\\_%' then (
                                                  case
                                                       when x1.unified_campaign_name similar to '%(\\\_InAppAction\\\_|\\\_InstallVolume\\\_|\\\_ConversionValue\\\_)%' then substring(x1.unified_campaign_name,regexp_instr(x1.unified_campaign_name,'_',1,9)+1,len(x1.unified_campaign_name))
                                                       else substring(x1.unified_campaign_name,regexp_instr(x1.unified_campaign_name,'_',1,8)+1,len(x1.unified_campaign_name))
                                                  end
                                             )
                                        end
                                 )
                                 when upper(source) = 'GOOGLE MARKETING PLATFORM (DV360)' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'REVX' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'APPLE SEARCH ADS' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 and x1.unified_campaign_name not similar to '(adquant\\\_){0,1}GP\\\_%' then substring(x1.unified_campaign_name,regexp_instr(x1.unified_campaign_name,'_',1,8)+1,len(x1.unified_campaign_name))
                                 when upper(source) = 'AURA IRONSOURCE' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then (
                                        case
                                             when x1.unified_campaign_name like '\\\_\\\_US\\\_%' then split_part(x1.unified_campaign_name,'_',11)
                                             else split_part(x1.unified_campaign_name,'_',9)
                                        end
                                 )
                                 when upper(source) = 'VALNET' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'THETRADEDESK' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when platform_V2 in ('ANDROIDTV','PLAYSTATION','PS4','PS5','COMCAST') and regexp_count(x1.unified_campaign_name, '_') = 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'LINEAR TV' and regexp_count(x1.unified_campaign_name,'\\\_') >= 8 then split_part(x1.unified_campaign_name,'_',9)
                                 when upper(source) = 'MOLOCO' then split_part(upper(x1.unified_campaign_name),'_',9)
                            end as campaign_Notes,
                            upper(campaign_city || campaign_state || campaign_country || campaign_targeting || campaign_bid_type || campaign_ad_placement || campaign_device || campaign_platform || campaign_publisher || campaign_delivery) as campaign_internal_id,
                            case when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',1)
                                                                     when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',2)
                                                                     when date(date) >= '2022-11-01' then regexp_substr(x1.creative_name,'[0-9]{6,}')
                                                                     when regexp_count(x1.creative_name, '[0-9]{6,}') = 2 then split_part(split_part(upper(x1.creative_name),'ROKU',2),'_',4)
                                                                     when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 then split_part(x1.creative_name,'_',4)
                                                                end
                                 when upper(source) = 'SAMSUNG' THEN case when DATE(DATE) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then trim(split_part(split_part(upper(x1.creative_name),'|',4),'_',2))
                                                                          when DATE(DATE) >= '2022-06-01' and regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then trim(split_part(split_part(upper(x1.creative_name),'|',4),'_',1))
                                                                          else case when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 then regexp_substr(x1.creative_name, '[0-9]{6,}') end
                                                                     end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',2)
                                                                                     else case when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 then regexp_substr(x1.creative_name, '[0-9]{6,}') end
                                                                                end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',2)
                                                                         when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 then regexp_substr(x1.creative_name, '[0-9]{6,}')
                                                                    end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',2)
                                                                       when DATE(DATE) >= '2022-07-15' then split_part(upper(x1.creative_name),'_',1)
                                                                  end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(upper(x1.creative_name),'_',1)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',2)
                                                                        when DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%CFORWARD' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{1,})(/)([0-9]{1,})(-)([0-9]{1,})(/)([0-9]{1,})(_)'),2),'_',1)
                                                                                                                                                                when upper(x1.creative_name) like '%CAROUSEL%' then regexp_substr(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'[0-9]{6,}')
                                                                                                                                                           end
                                                                        when DATE(DATE) >= '2022-06-01' then split_part(regexp_substr(upper(x1.creative_name), '([0-9]{5,})(_)([A-Z0-9]{1,})(_)([0-9]{3,})(X)([0-9]{3,})'),'_',1)
                                                                   end
                                 -- when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-09-01' then case when upper(x1.creative_name) like upper(campaign_name) || '%' then regexp_substr(split_part(upper(x1.creative_name),upper(campaign_name),2),'[0-9]{6,}')
                                 --                                                                       else regexp_substr(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'[0-9]{6,}')
                                 --                                                                  end
                                 -- when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',1)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',1)
                                 when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',1)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 1)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 2)
                                 when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 2)
                            end as campaign_content_id,
                            case when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',2)
                                                                     when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',3)
                                                                     when regexp_count(upper(x1.creative_name), '[0-9]{1,}(_)ROKU') = 1 then split_part(x1.creative_name,'_',4)
                                                                     when date(date) >= '2023-01-01' then split_part(x1.creative_name,'_',3)
                                                                     when regexp_count(x1.creative_name, '[0-9]{6,}') = 2 then split_part(split_part(upper(x1.creative_name),'ROKU',2),'_',3)
                                                                     else split_part(x1.creative_name,'_',3)
                                                                end
                                 -- when upper(source) = 'AMAZON MEDIA GROUP' then case when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 and regexp_instr(x1.creative_name,'( |_)[0-9]{6,}') - regexp_instr(x1.creative_name,'[0-9]{1,}(-|/)[0-9]{1,}(-|/)[0-9]{1,}_',1,1,1) > 0
                                 --                                                       then substring(x1.creative_name,regexp_instr(x1.creative_name,'[0-9]{1,}(-|/)[0-9]{1,}(-|/)[0-9]{1,}_',1,1,1),regexp_instr(x1.creative_name,'( |_)[0-9]{6,}') - regexp_instr(x1.creative_name,'[0-9]{1,}(-|/)[0-9]{1,}(-|/)[0-9]{1,}_',1,1,1) )
                                 --                                                end

                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',3)
                                                                                     else case when regexp_count(x1.creative_name, '[0-9]{6,}') = 1 then split_part(regexp_substr(upper(x1.creative_name), '(\\_)([a-zA-Z0-9 '']{1,})(\\_)([0-9]{6,})'),'_',2) end
                                                                                end
                                 --when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',6)
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',3)
                                                                        when DATE(DATE) >= '2022-06-01' then case when regexp_count(split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(x|X)([0-9]{3,})(_)([a-zA-Z0-9]{1,})(_)([a-zA-Z0-9]{1,}|)(_)([a-zA-Z0-9]{1,})'),'_',4),'-') = 1  then split_part(split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(x|X)([0-9]{3,})(_)([a-zA-Z0-9]{1,})(_)([a-zA-Z0-9]{1,}|)(_)([a-zA-Z0-9]{1,})'),'_',4),'-',2)
                                                                                                                  else split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(x|X)([0-9]{3,})(_)([a-zA-Z0-9]{1,})(_)([a-zA-Z0-9]{1,}|)(_)([a-zA-Z0-9]{1,})'),'_',4)
                                                                                                             end
                                                                        when upper(x1.creative_name) like '%TARGETING%' then split_part(split_part(upper(x1.creative_name),'TARGETING',2),'_',2)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_AV\\\_SUPPRESS\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-5_AV_SUPPRESS_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_SUPPRESS\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-5_SUPPRESS_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_SUPPRESS\\\_%' and regexp_instr(split_part(upper(x1.creative_name),'H1-5_SUPPRESS_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'H1-5_SUPPRESS_',2),1,regexp_instr(split_part(upper(x1.creative_name),'H1-5_SUPPRESS_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_SUPPRESS \\\_%' and regexp_instr(split_part(upper(x1.creative_name),'H1-5_SUPPRESS _',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'H1-5_SUPPRESS _',2),1,regexp_instr(split_part(upper(x1.creative_name),'H1-5_SUPPRESS _',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-5_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-6\\\_SUPPRESSCLICKS\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-6_SUPPRESSCLICKS_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-6\\\_SUPPRESS\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-6_SUPPRESS_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-6\\\_%' and upper(x1.creative_name) like '%\\\_AP\\\_%' then split_part(split_part(upper(x1.creative_name),'H1-6_',2),'_AP_',1)
                                                                        when upper(x1.creative_name) like '%H1-5\\\_%' and upper(x1.creative_name) like '%TN%' and regexp_instr(split_part(upper(x1.creative_name),'H1-5_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'H1-5_',2),1,regexp_instr(split_part(upper(x1.creative_name),'H1-5_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%H1-6\\\_%' and upper(x1.creative_name) like '%TN%' and regexp_instr(split_part(upper(x1.creative_name),'H1-6_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'H1-6_',2),1,regexp_instr(split_part(upper(x1.creative_name),'H1-6_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%D1-4\\\_SUPPRESS\\\_%' and regexp_instr(split_part(upper(x1.creative_name),'D1-4_SUPPRESS_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'D1-4_SUPPRESS_',2),1,regexp_instr(split_part(upper(x1.creative_name),'D1-4_SUPPRESS_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%D1-4\\\_AV\\\_SUPPRESS\\\_%' and regexp_instr(split_part(upper(x1.creative_name),'D1-4_AV_SUPPRESS_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'D1-4_AV_SUPPRESS_',2),1,regexp_instr(split_part(upper(x1.creative_name),'D1-4_AV_SUPPRESS_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%D1-4\\\_AV\\\_%' and regexp_instr(split_part(upper(x1.creative_name),'D1-4_AV_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'D1-4_AV_',2),1,regexp_instr(split_part(upper(x1.creative_name),'D1-4_AV_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%D1-4\\\_%' and regexp_instr(split_part(upper(x1.creative_name),'D1-4_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'D1-4_',2),1,regexp_instr(split_part(upper(x1.creative_name),'D1-4_',2),'_[0-9]{3}') - 1)
                                                                        when upper(x1.creative_name) like '%SUPPRESS%' and regexp_instr(split_part(upper(x1.creative_name),'SUPPRESS_',2),'_[0-9]{3}') - 1 > 0 then substring(split_part(upper(x1.creative_name),'SUPPRESS_',2),1,regexp_instr(split_part(upper(x1.creative_name),'SUPPRESS_',2),'_[0-9]{3}') - 1)
                                                                   end

                               when upper(source) = 'SAMSUNG' then case when DATE(DATE) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',3)
                                                                        when DATE(DATE) >= '2022-06-01' then case when regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then case when split_part(split_part(upper(x1.creative_name),'|',4),'_',6) like '%CONTENT-%' then replace(split_part(split_part(split_part(upper(x1.creative_name),'|',4),'_',6),'CONTENT-',2),'-','')
                                                                                                                                                                                                 else split_part(split_part(upper(x1.creative_name),'|',4),'_',6)
                                                                                                                                                                                            end
                                                                                                                  when regexp_count(split_part(upper(x1.creative_name),'|',5),'\\\_') = 5 then case when split_part(split_part(upper(x1.creative_name),'|',5),'_',6) like '%CONTENT-%' then replace(split_part(split_part(split_part(upper(x1.creative_name),'|',5),'_',6),'CONTENT-',2),'-','')
                                                                                                                                                                                                 else split_part(split_part(upper(x1.creative_name),'|',5),'_',6)
                                                                                                                                                                                            end
                                                                                                                  when regexp_count(split_part(upper(x1.creative_name),'|',6),'\\\_') = 5 then case when split_part(split_part(upper(x1.creative_name),'|',6),'_',6) like '%CONTENT-%' then replace(split_part(split_part(split_part(upper(x1.creative_name),'|',6),'_',6),'CONTENT-',2),'-','')
                                                                                                                                                                                                 else split_part(split_part(upper(x1.creative_name),'|',6),'_',6)
                                                                                                                                                                                             end
                                                                                                             end
                                                                        when regexp_count(x1.creative_name, '\\\|') = 2 then case when regexp_count(split_part(upper(x1.creative_name),'|',3), '-') = 1 then split_part(split_part(upper(x1.creative_name),'|',3),'-',1)
                                                                                                                                  else split_part(upper(x1.creative_name),'|',3)
                                                                                                                             end
                                                                        when regexp_count(x1.creative_name, '\\\|') = 3 then case when split_part(upper(x1.creative_name),'|',3) like '%CONTENT\\\_%' then split_part(split_part(upper(x1.creative_name),'|',3),'CONTENT_',2)
                                                                                                                                  when split_part(upper(x1.creative_name),'|',3) like '%ValueProp_TopWatch%' then '' -- THis is temporary as there isn't any indication of content in this creative
                                                                                                                                  when regexp_count(split_part(upper(x1.creative_name),'|',4), '-') = 2 then case when split_part(split_part(upper(x1.creative_name),'|',4),'-',2) like '%CONTENT\\\_%' then split_part(split_part(split_part(upper(x1.creative_name),'|',4),'-',2),'CONTENT_',2)
                                                                                                                                                                                                               else split_part(split_part(upper(x1.creative_name),'|',4),'-',2)
                                                                                                                                                                                                          end
                                                                                                                                  when split_part(upper(x1.creative_name),'|',4) similar to '%V1%|%V2%' then split_part(split_part(upper(x1.creative_name),'|',4),'-',1)
                                                                                                                                  when regexp_count(split_part(upper(x1.creative_name),'|',4), '-') = 1 then split_part(split_part(upper(x1.creative_name),'|',4),'-',2)
                                                                                                                                  when split_part(upper(x1.creative_name),'|',4) like '%\\\_%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',2)
                                                                                                                                  when split_part(upper(x1.creative_name),'|',4) like '%CONTENT %' then split_part(split_part(upper(x1.creative_name),'|',4),'CONTENT ',2)
                                                                                                                                  else split_part(upper(x1.creative_name),'|',4)
                                                                                                                             end
                                                                        when regexp_count(x1.creative_name, '\\\|') = 4 then case when trim(split_part(upper(x1.creative_name),'|',4)) like '%\\\_%' then split_part(trim(split_part(upper(x1.creative_name),'|',4)),'_',2)
                                                                                                                                  when trim(split_part(upper(x1.creative_name),'|',4)) like '%\\\-%' then split_part(trim(split_part(upper(x1.creative_name),'|',4)),'-',2)
                                                                                                                             end
                                                                   end

                               when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',3)
                                                                     when DATE(DATE) >= '2022-07-15' then split_part(upper(x1.creative_name),'_',6)
                                                                     when regexp_count(x1.creative_name, '[0-9]{8}') = 1 then case when split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '[0-9]{8}'),2),'_',2) = 'DA'
                                                                                                                                     or regexp_count(split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '[0-9]{8}'),2),'_',2),'\\\.') = 1 then ''
                                                                                                                                   else split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '[0-9]{8}'),2),'_',2)
                                                                                                                              end
                                                                end
                               when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',3)
                                                                       when DATE(DATE) >= '2022-07-01' and regexp_count(x1.creative_name, '\\\_') = 5 then split_part(upper(x1.creative_name),'_',6)
                                                                       when DATE(DATE) >= '2022-11-01' and regexp_count(x1.creative_name, '\\\_') = 4 then split_part(upper(x1.creative_name),'_',5)
                                                                  end
                               when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',6)
                               when upper(source) = 'REVX' THEN SPLIT_PART(SPLIT_PART(creative_name, '_', 6),' ',1)
                               when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 3)
                               when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 3)
                            end as campaign_content_name,
                            case when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',3)
                                                                     when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',4)
                                                                     else null
                                                                end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',4)
                                                                                     else null
                                                                                end
                                when upper(source) = 'SAMSUNG' then case when DATE(DATE) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',4)
                                                                         else null
                                                                    end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',4)
                                                                        else null
                                                                   end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',4)
                                                                       else null
                                                                  end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',4)
                                                                       else null
                                                                  end
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 4)
                                 when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 4)
                                 else NULL
                            end as campaign_creative_language,
                            case when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',5)
                                                                       when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',6)
                                                                       when regexp_count(upper(x1.creative_name), '[0-9]{1,}(_)ROKU') = 1 then split_part(x1.creative_name,'_',7)
                                                                       when date(date) >= '2023-01-01' then split_part(x1.creative_name,'_',6)
                                                                       when regexp_count(x1.creative_name, '[0-9]{6,}') = 2 then split_part(split_part(upper(x1.creative_name),'ROKU',2),'_',6)
                                                                       else split_part(x1.creative_name,'_',6)
                                                                  end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',6)
                                                                                else null
                                                                                end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(upper(x1.creative_name),'_',2)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',2)
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',6)
                                                                         when DATE(DATE) >= '2022-07-01' and regexp_count(x1.creative_name, '\\\_') = 5 then split_part(upper(x1.creative_name),'_',2)
                                                                         when DATE(DATE) >= '2022-11-01' and regexp_count(x1.creative_name, '\\\_') = 4 then split_part(upper(x1.creative_name),'_',2)
                                                                    end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',6)
                                                                       when DATE(DATE) >= '2022-07-15' then split_part(upper(x1.creative_name),'_',2)
                                                                  end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',6)
                                                                        when DATE(DATE) >= '2022-06-01' then split_part(regexp_substr(upper(x1.creative_name), '([A-Z0-9]{1,})(_)([0-9]{3,})(X)([0-9]{3,})'),'_',1)
                                                                        else null
                                                                   end
                                 --when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',2)
                                 when upper(source) = 'SAMSUNG' then case when DATE(date) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',6)
                                                                          when DATE(DATE) >= '2022-06-01' then case when regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',4),'_',2)
                                                                                                                    when regexp_count(split_part(upper(x1.creative_name),'|',5),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',5),'_',2)
                                                                                                                    when regexp_count(split_part(upper(x1.creative_name),'|',6),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',6),'_',2)
                                                                                                               end
                                                                     end
                                 when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',2)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 2)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 6)
                                 when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 6)
                            end as campaign_creative_category,
                            case when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',7)
                                                                                else regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3,})',1,1,'i')
                                                                                end
                                 when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',6)
                                                                     when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',7)
                                                                     else regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3,})',1,1,'i')
                                                                end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',7)
                                                                        when upper(source) = 'VIZIO' then regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3,})',1,1,'i')
                                                                        when DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',3)
                                                                        when upper(x1.creative_name) similar to ('%H1-5%|%H1-6%|%H1-H6%') or upper(x1.unified_campaign_name) similar to ('%H1-5%|%H1-6%|%H1-H6%') then '1560x390'
                                                                        when upper(x1.creative_name) similar to ('%D1-4%|%D1-6%|%D1-5%') or upper(x1.unified_campaign_name) similar to ('%D1-4%|%D1-6%|%D1-5%') then '378x216' --carosel - 378x216
                                                                   end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',7)
                                                                       else regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3,})',1,1,'i')
                                                                  end
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',7)
                                                                       else regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3})',1,1,'i')
                                                                  end
                                 when upper(source) = 'SAMSUNG' then case when DATE(date) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',7)
                                                                          else regexp_substr(x1.creative_name, '([0-9]{3,})(x)([0-9]{3})',1,1,'i')
                                                                     end
                                 when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(x1.creative_name,'_',3)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',3)
                                 when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',3)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 3)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 7)
                                 when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 7)
                            end as campaign_creative_size,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(x1.creative_name,'_',4)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',4)
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',8)
                                                                         when DATE(DATE) >= '2022-07-01' and regexp_count(x1.creative_name, '\\\_') = 5 then split_part(upper(x1.creative_name),'_',4)
                                                                         when DATE(DATE) >= '2022-11-01' and regexp_count(x1.creative_name, '\\\_') = 4 then split_part(upper(x1.creative_name),'_',4)
                                                                    end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',8)
                                                                       when DATE(DATE) >= '2022-07-15' then split_part(upper(x1.creative_name),'_',4)
                                                                  end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',8)
                                                                        when DATE(DATE) >= '2022-06-01' then case when upper(x1.creative_name) like '%CAROUSEL%CFORWARD' then 'IMAGE'
                                                                                                                  else split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(X)([0-9]{3,})(_)([A-Z0-9]{1,})'),'_',2)
                                                                                                             end
                                                                   end
                                 -- when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' and upper(x1.creative_name) like '%CAROUSEL%' then case when upper(x1.creative_name) like '%CAROUSEL%CFORWARD' then 'IMAGE'
                                 --                                                                       else split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',4)
                                 --                                                                  end
                                 when upper(source) = 'SAMSUNG' then case when DATE(date) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',8)
                                                                          when DATE(DATE) >= '2022-06-01' then case when regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',4),'_',4)
                                                                                                                    when regexp_count(split_part(upper(x1.creative_name),'|',5),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',5),'_',4)
                                                                                                                    when regexp_count(split_part(upper(x1.creative_name),'|',6),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',6),'_',4)
                                                                                                               end
                                                                     end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',8)
                                                                                else null
                                                                                end
                                 when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',7)
                                                                       when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',8)
                                                                       else null
                                                                  end
                                 when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',4)
                                 when channel_group = 'Google App Campaigns' then case when adgroup_name ilike '%_image_%'   THEN 'IMAGE'
                                                                                       when adgroup_name ilike '%_video_%'   THEN 'VIDEO'
                                                                                       when adgroup_name ilike '%_gif_%'     THEN 'GIF'
                                                                                       when adgroup_name ilike '%_trailer_%' THEN 'TRAILER'
                                                                                       else null
                                                                                  end
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 4)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 8)
                                 when upper(source) = 'MOLOCO' THEN split_part(creative_name, '_', 8)
                                 else case when x1.creative_name ilike '%_image_%'   THEN 'IMAGE'
                                           when x1.creative_name ilike '%_video_%'   THEN 'VIDEO'
                                           when x1.creative_name ilike '%_gif_%'     THEN 'GIF'
                                           when x1.creative_name ilike '%_trailer_%' THEN 'TRAILER'
                                           else null
                                      end
                            end as campaign_creative_type,

                            -- case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(x1.creative_name,'_',5)
                            --      when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',5)
                            --      when upper(source) = 'LG ADS' AND DATE(DATE) >= '2022-07-01' and regexp_count(x1.creative_name, '\\\_') = 5 then split_part(upper(x1.creative_name),'_',5)
                            --      when upper(source) = 'XBOX' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') = 7 then split_part(upper(x1.creative_name),'_',5)
                            --      when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(x|X)([0-9]{3,})(_)([a-zA-Z]{1,})(_)([a-zA-Z0-9]{1,}|)(_)([a-zA-Z0-9]{1,})'),'_',3)
                            --      -- when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',5)
                            --      when upper(source) = 'SAMSUNG' and  DATE(DATE) >= '2022-06-01' then case when regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',4),'_',5)
                            --                                                                               when regexp_count(split_part(upper(x1.creative_name),'|',5),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',5),'_',5)
                            --                                                                               when regexp_count(split_part(upper(x1.creative_name),'|',6),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',6),'_',5)
                            --                                                                          end
                            --      else regexp_substr(lower(x1.creative_name), '([0-9a-z]{8})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{12})')
                            --
                            --
                            -- end as campaign_creative_UUID,
                            case when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',8)
                                                                       when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',10)
                                                                       else regexp_substr(lower(x1.creative_name), '([0-9a-z]{8})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{12})')
                                                                  end
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 5)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 9)
                                 else regexp_substr(lower(x1.creative_name), '([0-9a-z]{8})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{12})')
                            end as campaign_creative_UUID,

                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.creative_name,'\\\_') >= 6 then split_part(x1.creative_name,'_',6)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.creative_name,'\\\_') >= 5 then split_part(x1.creative_name,'_',6)
                                 when upper(source) = 'LG ADS' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'lg\\\_%' THEN split_part(x1.creative_name,'_',9)
                                                                         when DATE(DATE) >= '2022-07-01' and regexp_count(x1.creative_name, '\\\_') = 5 then split_part(upper(x1.creative_name),'_',6)
                                                                         when DATE(DATE) >= '2022-11-01' and regexp_count(x1.creative_name, '\\\_') = 4 then split_part(upper(x1.creative_name),'_',5)
                                                                    end
                                 when upper(source) = 'VIZIO' then case when DATE(date) >= '2023-09-01' then split_part(split_part(upper(x1.creative_name),upper(x1.unified_campaign_name)||'_',2),'_',9)
                                                                        when DATE(DATE) >= '2022-06-01' then split_part(regexp_substr(upper(x1.creative_name), '([0-9]{3,})(x|X)([0-9]{3,})(_)([a-zA-Z0-9]{1,})(_)([a-zA-Z0-9]{1,}|)(_)([a-zA-Z0-9]{1,})'),'_',4)
                                                                   end
                                 --when upper(source) = 'VIZIO' AND DATE(DATE) >= '2022-06-01' then split_part(split_part(upper(x1.creative_name),regexp_substr(upper(x1.creative_name), '([0-9]{4})(-|_)([0-9]{4})(_)'),2),'_',6)
                                 when upper(source) = 'SAMSUNG' then case when DATE(date) >= '2023-09-01' and trim(split_part(upper(x1.creative_name),'|',4)) ilike 'samsung%' then split_part(split_part(upper(x1.creative_name),'|',4),'_',9)
                                                                          when DATE(DATE) >= '2022-06-01' and regexp_count(split_part(upper(x1.creative_name),'|',4),'\\\_') = 5 then split_part(split_part(upper(x1.creative_name),'|',4),'_',6)
                                                                     end
                                 when upper(source) = 'AMAZON MEDIA GROUP' then case when date(date) >= '2023-08-01' and x1.creative_name ilike 'amazon\\\_%' THEN split_part(x1.creative_name,'_',9)
                                                                                else null
                                                                                end
                                 when platform_V2 = 'ROKU' then case when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') = 8 and x1.creative_name not ilike 'roku%' then split_part(x1.creative_name,'_',9)
                                                                       when date(date) >= '2023-08-01' and regexp_count(x1.creative_name, '_') >= 8                                        then split_part(x1.creative_name,'_',9)
                                                                       else null
                                                                  end
                                 when upper(source) = 'XBOX' then case when DATE(date) >= '2023-09-01' and x1.creative_name ilike 'xbox\\\_%' THEN split_part(x1.creative_name,'_',9)
                                                                     else null
                                                                end
                                 when upper(source) in ('PLAYSTATION','PS4','PS5') and regexp_count(x1.creative_name, '_') = 5 then split_part(x1.creative_name,'_',6)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(creative_name, '_', 6)
                                 when upper(source) IN ('COMCAST','PS4', 'PS5') THEN SPLIT_PART(creative_name, '_', 10)
                                 when upper(source) = 'MOLOCO' THEN split_part(x1.creative_name,'_',9)
                            end as campaign_creative_notes,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',1)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',1)
                                 when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.sub_campaign_name,'_',1)
                                 when platform_V2 = 'WEB' and DATE(DATE) >= '2023-02-01' then split_part(x1.sub_campaign_name,'_',1)
                                 when platform_V2 = 'ANDROID' AND source = 'AdWords' AND regexp_count(sub_campaign_name,'\\\_') >= 6 THEN replace(split_part(sub_campaign_name, '_', 1), 'zzz', '')

                            end as campaign_adgroup_content_id,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',2)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',2)
                                 when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.sub_campaign_name,'_',4)
                                 when platform_V2 = 'WEB' and DATE(DATE) >= '2023-02-01' then split_part(x1.sub_campaign_name,'_',2)
                                 when platform_V2 = 'ANDROID' AND source = 'AdWords' AND regexp_count(sub_campaign_name,'\\\_') >= 6 THEN split_part(sub_campaign_name, '_', 2)

                            end as campaign_adgroup_content_title,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',3)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',3)
                                 when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.sub_campaign_name,'_',3)
                                 when platform_V2 = 'WEB' and DATE(DATE) >= '2023-02-01' then split_part(x1.sub_campaign_name,'_',3)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(sub_campaign_name, '_', 3)
                            end as campaign_adgroup_targeting,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',4)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',4)
                                 when platform_V2 = 'WEB' and DATE(DATE) >= '2023-02-01' then split_part(x1.sub_campaign_name,'_',4)

                            end as campaign_adgroup_launch_date,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',5)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',5)
                                 else regexp_substr(lower(x1.sub_campaign_name), '([0-9A-Z0-9]{8})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{4})(-)([0-9a-z]{12})')

                            end as campaign_adgroup_UUID,
                            case when upper(source) = 'TIKTOK ADS' and DATE(DATE) >= '2022-07-15' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',6)
                                 when upper(source) = 'FACEBOOK' and DATE(DATE) >= '2022-10-01' and regexp_count(x1.sub_campaign_name,'\\\_') >= 5 then split_part(x1.sub_campaign_name,'_',6)
                                 when platform_V2 = 'YOUTUBE CTV' and DATE(DATE) >= '2023-02-03' then split_part(x1.sub_campaign_name,'_',5)
                                 when platform_V2 = 'WEB' and DATE(DATE) >= '2023-02-01' then split_part(x1.sub_campaign_name,'_',6)
                                 when upper(source) = 'REVX' THEN SPLIT_PART(sub_campaign_name, '_', 6)
                            end as campaign_adgroup_notes,
                            adn_account_id,
                            adn_subadnetwork,
                            keyword,
                            adn_account_name,
                            adn_campaign_id,
                            tracker_campaign_id,
                            bid_type,
                            bid_strategy,
                            bid_amount,
                            campaign_objective,
                            standardized_bid_type,
                            standardized_bid_strategy,
                            original_bid_amount,
                            campaign_status,
                            keyword_id,
                            custom_impressions,
                            completed_video_view_rate,
                            custom_clicks,
                            adn_clicks,
                            tracker_clicks,
                            clicks_discrepancy,
                            custom_installs,
                            adn_installs,
                            tracker_installs,
                            installs_discrepancy,
                            adn_cost,
                            case when (date(date) between '2022-01-01' and '2022-08-31') and upper(source) in ('TIKTOK ADS','SNAPCHAT','FACEBOOK') then adn_cost * .035
                                 when date(date) >= '2022-01-01' and (upper(source) in ('ADWORDS')
                                                                 or upper(x1.unified_campaign_name) ilike '%YOUTUBE%CTV%'
                                                                 or upper(source) like '%APPLE%') then adn_cost * .035
                                 else 0
                            end as agency_fee,
                            adn_cost + agency_fee as total_cost,
                            video_views,
                            completed_video_views,
                            video_views_25pct,
                            video_views_50pct,
                            video_views_75pct,
                            revenue_actual,
                            original_revenue_actual,
                            arpu_actual,
                            roi_actual,
                            start_video_cpe_actual,
                            tvt_proxy_ecvr_actual,
                            10min_played_cpe_actual,
                            start_video_ecvr_actual,
                            10min_played_ecvr_actual,
                            tvt_proxy_cpe_actual


                      from spectrum.singular_granular_campaign_details x1


                      -- created in Data Bricks from a csv, you can find it under the name Country Codes
                      left join temp.country_codes_csv x3
                      on x1.country_field = x3.three_digit_country
                    )

, singular_etl_clean AS (
select x1.etl_query_timestamp_utc,
       x1.Skan_campaign_id,
       x1.ds,
       x1.app,
       x1.data_source,
       x1.source,
       x1.OS,
       x1.platform,
       x1.platform_V2,
       x1.channel_group,
       x1.platform_type,
       x1.country,
       x1.country_singular,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_id
            else x1.campaign_id
       end as campaign_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_name
            else x1.campaign_name
       end as campaign_name,
       case when coalesce(x2.platform_V2,'') != '' then x2.creative_id
            else x1.creative_id
       end as creative_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.creative_name
            else x1.creative_name
       end as creative_name,
       case when coalesce(x2.platform_V2,'') != '' then x2.adgroup_id
            else x1.adgroup_id
       end as adgroup_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.adgroup_name
            else x1.adgroup_name
       end as adgroup_name,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_city
            else x1.campaign_city
       end as campaign_city,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_state
            else x1.campaign_state
       end as campaign_state,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_country
            else x1.campaign_country
       end as campaign_country,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_targeting_group
            else x1.campaign_targeting_group
       end as campaign_targeting_group,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_targeting
            else x1.campaign_targeting
       end as campaign_targeting,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_bid_type
            else x1.campaign_bid_type
       end as campaign_bid_type,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_ad_placement
            else x1.campaign_ad_placement
       end as campaign_ad_placement,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_device
            else x1.campaign_device
       end as campaign_device,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_platform
            else x1.campaign_platform
       end as campaign_platform,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_publisher
            else x1.campaign_publisher
       end as campaign_publisher,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_type
            else x1.campaign_type
       end as campaign_type,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_Delivery
            else x1.campaign_Delivery
       end as campaign_Delivery,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_Optimization_Event
            else x1.campaign_Optimization_Event
       end as campaign_Optimization_Event,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_notes
            else x1.campaign_notes
       end as campaign_notes,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_internal_id
            else x1.campaign_internal_id
       end as campaign_internal_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_content_id
            else x1.campaign_content_id
       end as campaign_content_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_content_name
            else x1.campaign_content_name
       end as campaign_content_name,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_language
            else x1.campaign_creative_language
       end as campaign_creative_language,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_category
            else x1.campaign_creative_category
       end as campaign_creative_category,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_size
            else x1.campaign_creative_size
       end as campaign_creative_size,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_type
            else x1.campaign_creative_type
       end as campaign_creative_type,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_UUID
            else x1.campaign_creative_UUID
       end as campaign_creative_UUID,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_creative_notes
            else x1.campaign_creative_notes
       end as campaign_creative_notes,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_content_id
            else x1.campaign_adgroup_content_id
       end as campaign_adgroup_content_id,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_content_title
            else x1.campaign_adgroup_content_title
       end as campaign_adgroup_content_title,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_targeting
            else x1.campaign_adgroup_targeting
       end as campaign_adgroup_targeting,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_launch_date
            else x1.campaign_adgroup_launch_date
       end as campaign_adgroup_launch_date,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_UUID
            else x1.campaign_adgroup_UUID
       end as campaign_adgroup_UUID,
       case when coalesce(x2.platform_V2,'') != '' then x2.campaign_adgroup_notes
            else x1.campaign_adgroup_notes
       end as campaign_adgroup_notes,
       x1.adn_account_id,
       x1.adn_subadnetwork,
       x1.keyword,
       x1.adn_account_name,
       x1.adn_campaign_id,
       x1.tracker_campaign_id,
       x1.bid_type,
       x1.bid_strategy,
       x1.bid_amount,
       x1.campaign_objective,
       x1.standardized_bid_type,
       x1.standardized_bid_strategy,
       x1.original_bid_amount,
       x1.campaign_status,
       x1.keyword_id,
       x1.custom_impressions,
       x1.completed_video_view_rate,
       x1.custom_clicks,
       x1.adn_clicks,
       x1.tracker_clicks,
       x1.clicks_discrepancy,
       x1.custom_installs,
       x1.adn_installs,
       x1.tracker_installs,
       x1.installs_discrepancy,
       x1.team_spend,
       x1.adn_cost,
       x1.agency_fee,
       x1.total_cost,
       x1.video_views,
       x1.completed_video_views,
       x1.video_views_25pct,
       x1.video_views_50pct,
       x1.video_views_75pct,
       x1.revenue_actual,
       x1.original_revenue_actual,
       x1.arpu_actual,
       x1.roi_actual,
       x1.start_video_cpe_actual,
       x1.tvt_proxy_ecvr_actual,
       10min_played_cpe_actual,
       x1.start_video_ecvr_actual,
       10min_played_ecvr_actual,
       x1.tvt_proxy_cpe_actual

from singular_etl x1

left join (
            SELECT distinct
                   platform_V2,
                   channel_group,
                   campaign_id,
                   last_value(campaign_name) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_name,
                   creative_id,
                   last_value(creative_name) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as creative_name,
                   adgroup_id,
                   last_value(adgroup_name) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as adgroup_name,
                   last_value(campaign_city) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_city,
                   last_value(campaign_State) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_state,
                   last_value(campaign_country) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_country,
                   last_value(campaign_targeting_group) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_targeting_group,
                   last_value(campaign_targeting) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_targeting,
                   last_value(campaign_bid_type) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_bid_type,
                   last_value(campaign_ad_placement) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_ad_placement,
                   last_value(campaign_device) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_device,
                   last_value(campaign_platform) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_platform,
                   last_value(campaign_publisher) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_publisher,
                   last_value(campaign_type) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_type,
                   last_value(campaign_delivery) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_delivery,
                   last_value(campaign_Optimization_Event) over (partition by platform_v2,channel_group,campaign_id order by ds rows between unbounded preceding and unbounded following) as campaign_Optimization_Event,
                   last_value(campaign_notes) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_notes,
                   last_value(campaign_internal_id) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_internal_id,
                   last_value(campaign_content_id) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_content_id,
                   last_value(campaign_content_name) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_content_name,
                   last_value(campaign_creative_language) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_language,
                   last_value(campaign_creative_category) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_category,
                   last_value(campaign_creative_size) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_size,
                   last_value(campaign_creative_type) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_type,
                   last_value(campaign_creative_UUID) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_UUID,
                   last_value(campaign_creative_notes) over (partition by platform_v2,channel_group,campaign_id,creative_id order by ds rows between unbounded preceding and unbounded following) as campaign_creative_notes,
                   last_value(campaign_adgroup_content_id) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_content_id,
                   last_value(campaign_adgroup_content_title) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_content_title,
                   last_value(campaign_adgroup_targeting) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_targeting,
                   last_value(campaign_adgroup_launch_date) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_launch_date,
                   last_value(campaign_adgroup_UUID) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_UUID,
                   last_value(campaign_adgroup_notes) over (partition by platform_v2,channel_group,campaign_id,adgroup_id order by ds rows between unbounded preceding and unbounded following) as campaign_adgroup_notes
            from singular_etl
          ) x2

on coalesce(x1.platform_v2,'') = coalesce(x2.platform_v2,'')
and coalesce(x1.channel_group,'')= coalesce(x2.channel_group,'')
and coalesce(x1.campaign_id,'')= coalesce(x2.campaign_id,'')
and coalesce(x1.creative_id,'') = coalesce(x2.creative_id,'')
and coalesce(x1.adgroup_id,'') = coalesce(x2.adgroup_id,'')
),
preFinal_CTE AS (

  SELECT
       x.etl_query_timestamp_utc,
       x.Skan_campaign_id,
       x.ds,
       x.app,
       x.data_source,
       x.source,
       x.OS,
       x.platform,
       x.platform_V2,
       x.channel_group,
       x.platform_type,
       x.country,
       x.country_singular,
       x.campaign_id,
       x.campaign_name,
       x.creative_id,
       x.creative_name,
       x.adgroup_id,
       x.adgroup_name,
       x.campaign_city,
       x.campaign_state,
       coalesce(gc.campaign_country, x.campaign_country) as campaign_country,
       coalesce(gc.campaign_targeting_group, x.campaign_targeting_group) as campaign_targeting_group,
       x.campaign_targeting,
       coalesce(gc.campaign_bid_type, x.campaign_bid_type) as campaign_bid_type,
       coalesce(gc.campaign_ad_placement, x.campaign_ad_placement) as campaign_ad_placement,
       x.campaign_device,
       x.campaign_platform,
       x.campaign_publisher,
       coalesce(gc.campaign_type, x.campaign_type) as campaign_type,
       coalesce(gc.campaign_delivery, x.campaign_Delivery) as campaign_delivery,
       x.campaign_Optimization_Event,
       x.campaign_notes,
       x.campaign_internal_id,
       coalesce(gc.campaign_content_id, x.campaign_content_id) as campaign_content_id,
       coalesce(gc.campaign_content_name, x.campaign_content_name) as campaign_content_name,
       x.campaign_creative_language,
       coalesce(gc.campaign_creative_category, x.campaign_creative_category) as campaign_creative_category,
       x.campaign_creative_size,
       coalesce(gc.campaign_creative_type, x.campaign_creative_type) as campaign_creative_type,
       x.campaign_creative_UUID,
       x.campaign_creative_notes,
       x.campaign_adgroup_content_id,
       x.campaign_adgroup_content_title,
       x.campaign_adgroup_targeting,
       x.campaign_adgroup_launch_date,
       x.campaign_adgroup_UUID,
       x.campaign_adgroup_notes,
       x.adn_account_id,
       x.adn_subadnetwork,
       x.keyword,
       x.adn_account_name,
       x.adn_campaign_id,
       x.tracker_campaign_id,
       x.bid_type,
       x.bid_strategy,
       x.bid_amount,
       x.campaign_objective,
       x.standardized_bid_type,
       x.standardized_bid_strategy,
       x.original_bid_amount,
       x.campaign_status,
       x.keyword_id,
       x.custom_impressions,
       x.completed_video_view_rate,
       x.custom_clicks,
       x.adn_clicks,
       x.tracker_clicks,
       x.clicks_discrepancy,
       x.custom_installs,
       x.adn_installs,
       x.tracker_installs,
       x.installs_discrepancy,
       x.team_spend,
       x.adn_cost,
       x.agency_fee,
       x.total_cost,
       x.video_views,
       x.completed_video_views,
       x.video_views_25pct,
       x.video_views_50pct,
       x.video_views_75pct,
       x.revenue_actual,
       x.original_revenue_actual,
       x.arpu_actual,
       x.roi_actual,
       x.start_video_cpe_actual,
       x.tvt_proxy_ecvr_actual,
       10min_played_cpe_actual,
       x.start_video_ecvr_actual,
       10min_played_ecvr_actual,
       x.tvt_proxy_cpe_actual
    FROM singular_etl_clean x
    LEFT JOIN temp.growth_campaign_lookup_historical gc
     ON upper(trim(x.platform_V2)) = upper(trim(gc.platform))
          AND upper(trim(x.channel_group)) = upper(trim(gc.channel_group))
          AND upper(trim(x.campaign_id)) = upper(trim(gc.campaign_id))
          AND upper(trim(x.creative_id)) = upper(trim(gc.creative_id))
)
SELECT
  x1.etl_query_timestamp_utc
, x1.Skan_campaign_id
, x1.ds
, x1.app
, x1.data_source
, x1.source
, x1.OS
, x1.platform
, x1.platform_V2
, coalesce(trim(x1.channel_group),'') as channel_group
, x1.platform_type
, x1.country
, x1.country_singular
, CASE 
     WHEN x1.platform_type = 'OTT' THEN upper(coalesce(trim(x1.campaign_country),''))
     ELSE x1.country_singular
     END as country_delivery
, coalesce(trim(x1.campaign_id),'') as campaign_id
, coalesce(trim(x1.campaign_name),'') as campaign_name
, coalesce(trim(x1.creative_id),'') as creative_id
, coalesce(trim(x1.creative_name),'') as creative_name
, coalesce(trim(x1.adgroup_id),'') as adgroup_id
, coalesce(trim(x1.adgroup_name),'') as adgroup_name
, upper(coalesce(trim(x1.campaign_city),'')) as campaign_city
, upper(coalesce(trim(x1.campaign_state),'')) as campaign_state
, upper(coalesce(trim(x1.campaign_country),'')) as campaign_country
, upper(coalesce(trim(x1.campaign_targeting_group),'')) as campaign_targeting_group
, upper(coalesce(trim(x1.campaign_targeting),'')) as campaign_targeting
, upper(coalesce(trim(x1.campaign_bid_type),'')) as campaign_bid_type
, upper(coalesce(trim(x1.campaign_ad_placement),'')) as campaign_ad_placement
, upper(coalesce(trim(x1.campaign_device),'')) as campaign_device
, upper(coalesce(trim(x1.campaign_platform),'')) as campaign_platform
, upper(coalesce(trim(x1.campaign_publisher),'')) as campaign_publisher
, upper(coalesce(trim(x1.campaign_type),'')) as campaign_type
, upper(coalesce(trim(x1.campaign_Delivery),'')) as campaign_Delivery
, upper(coalesce(trim(x1.campaign_Optimization_Event),'')) as campaign_Optimization_Event
, upper(coalesce(trim(x1.campaign_notes),'')) as campaign_notes
, upper(coalesce(trim(x1.campaign_internal_id),'')) as campaign_internal_id
, upper(coalesce(trim(x1.campaign_content_id),'')) as campaign_content_id
, upper(coalesce(trim(x2.content_type),trim(x3.content_type),'')) as campaign_content_type
, upper(coalesce(trim(x1.campaign_content_name),'')) as campaign_content_name
, coalesce(trim(x2.program_id),trim(x3.program_id),'') as campaign_creative_program_id
, coalesce(trim(x2.program_name),trim(x3.program_name),'') as campaign_creative_program_name
, upper(coalesce(trim(x1.campaign_creative_language),'')) as campaign_creative_language
, upper(coalesce(trim(x1.campaign_creative_category),'')) as campaign_creative_category
, upper(coalesce(trim(x1.campaign_creative_size),'')) as campaign_creative_size
, upper(coalesce(trim(x1.campaign_creative_type),'')) as campaign_creative_type
, coalesce(trim(x1.campaign_creative_UUID),'') as campaign_creative_UUID
, upper(coalesce(trim(x1.campaign_creative_notes),'')) as campaign_creative_notes
, upper(coalesce(trim(x1.campaign_adgroup_content_id),'')) as campaign_adgroup_content_id
, upper(coalesce(trim(x1.campaign_adgroup_content_title),'')) as campaign_adgroup_content_title
, upper(coalesce(trim(x1.campaign_adgroup_targeting),'')) as campaign_adgroup_targeting
, upper(coalesce(trim(x1.campaign_adgroup_launch_date),'')) as campaign_adgroup_launch_date
, coalesce(trim(x1.campaign_adgroup_UUID),'') as campaign_adgroup_UUID
, upper(coalesce(trim(x1.campaign_adgroup_notes),'')) as campaign_adgroup_notes
, CASE 
     -- For Vizio campaign, only apply logic to campaign after 8/1/2022 until campaign overwrite is removed
     -- because before that campaign id column was actually order id, which is a parent id to multiple campaigns
     -- so the camapign name was overwritten in an incorrect way
     WHEN x1.platform_v2 = 'VIZIO' AND ds < '2022-08-01' THEN 'PAID'
     WHEN UPPER(x1.campaign_name) LIKE '%ADDED%VALUE%' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%AV' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%(AV %' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '% AV)%' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%\\\_AV\\\_%' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%-AV\\\_%' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%\\\_PAIP' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%\\\_BONUS' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%\\\-BONUS' THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%| BONUS%' THEN 'ADDEDVALUE'
     WHEN x1.platform_v2='AMAZON' 
               AND x1.campaign_id IN ('582652764368066520','584343503009444708','587772411271223635','590484766449441413','590110296337830732') THEN 'ADDEDVALUE'
     WHEN x1.platform_v2='ROKU' 
               AND x1.campaign_id IN ('6050571497','6050573195','6169872904','6175542213','6181944709','6222716339','6246633574','6276135568','6390049340','6672778072','6691242846') THEN 'ADDEDVALUE'
     WHEN x1.platform_v2='SAMSUNG' 
               AND x1.campaign_id IN ('112548','112549','117475','144047','144048','170203','179228','186342') THEN 'ADDEDVALUE'
     WHEN x1.platform_v2='VIZIO' 
               AND x1.campaign_id IN ('1088093','1088094','1088095','1089979','1089981','1125734','1125757','1125758','1131788','1171095','1182835','004001001','1284625','2404004008001','1290666','1291038','1298446','2405004009001','1319739','1321915','1321918','1321919','1324633','1324661','1324678','1324693','1330194','1336265','1336301','1340716','1340797','1340798','1344074','1347022','1397822','1397829','1397990','1397991','1397992','1397993','1397994','1397996','1397997','1398016','1398377','1398450','1398452','1398455','1398457','1398458','1406830','1406922','1409064','1409082','1409083','1409084','1409085','1410180','1410185','1410203','1410222','1410230','1410253','1410274','1416019','1416021') THEN 'ADDEDVALUE'
     WHEN x1.platform_v2='LGTV' 
               AND x1.campaign_id IN ('6fc76b5055bbe1653e76a89606b0f42a0a7e3056','7eada466380fcc68fba89cc0b7259592f8c8a6ed','914407fad99e8e9e205a77296a11d3215a0c59d5','b94ef6d5ba7e65dbfc35db766a3e7239eacd4986','6bf9ae362666da3fc5fa6283995f87ae3a063c6b','c97edb6ddec12f8e3bd8e5b2cf19f4d9930da940','cad60a056eb437e6ec4866e2e81cc2c34b5a5bb3') THEN 'ADDEDVALUE'
     WHEN UPPER(x1.campaign_name) LIKE '%MAKE%GOOD%' THEN 'MAKEGOOD'
     WHEN UPPER(x1.campaign_name) LIKE '%\\\_MG' THEN 'MAKEGOOD'
     WHEN UPPER(x1.campaign_name) LIKE '%-MG' THEN 'MAKEGOOD'
     WHEN UPPER(x1.campaign_name) LIKE '%- MG' THEN 'MAKEGOOD'
     ElSE 'PAID'
     END AS campaign_spend_type
, CASE WHEN ect.targeting_type IS NOT NULL THEN UPPER(ect.targeting_type) ELSE upper(coalesce(trim(x1.campaign_targeting),'')) END as campaign_targeting_type
, CASE WHEN toc.ids IS NOT NULL THEN 1 ELSE 0 END as is_tubi_original
, CASE WHEN top2.ids IS NOT NULL THEN top2.top_1_genre ELSE '' END as creative_content_top1_genre
, CASE WHEN top2.ids IS NOT NULL THEN top2.top_2_genre ELSE '' END as creative_content_top2_genre
, x1.adn_account_id
, x1.adn_subadnetwork
, x1.keyword
, x1.adn_account_name
, x1.adn_campaign_id
, x1.tracker_campaign_id
, x1.bid_type
, x1.bid_strategy
, x1.bid_amount
, x1.campaign_objective
, x1.standardized_bid_type
, x1.standardized_bid_strategy
, x1.original_bid_amount
, x1.campaign_status
, x1.keyword_id
, x1.custom_impressions
, x1.completed_video_view_rate
, x1.custom_clicks
, x1.adn_clicks
, x1.tracker_clicks
, x1.clicks_discrepancy
, x1.custom_installs
, x1.adn_installs
, x1.tracker_installs
, x1.installs_discrepancy
, x1.team_spend
, CASE WHEN x1.source = 'Amazon Media Group' AND x1.country = 'GB' THEN x1.adn_cost*1.258 
       WHEN x1.source = 'Amazon Media Group' AND x1.country = 'MX' THEN x1.adn_cost*0.05
     ELSE x1.adn_cost END adn_cost
, CASE WHEN x1.source = 'Amazon Media Group' AND x1.country = 'GB' THEN x1.agency_fee*1.258 
       WHEN x1.source = 'Amazon Media Group' AND x1.country = 'MX' THEN x1.agency_fee*0.05
     ELSE x1.agency_fee END agency_fee
, CASE WHEN x1.source = 'Amazon Media Group' AND x1.country = 'GB' THEN x1.total_cost*1.258 
       WHEN x1.source = 'Amazon Media Group' AND x1.country = 'MX' THEN x1.total_cost*0.05
     ELSE x1.total_cost END total_cost
, CASE WHEN x1.source = 'Amazon Media Group' AND x1.country = 'GB' THEN x1.total_cost*1.258 
       WHEN x1.source = 'Amazon Media Group' AND x1.country = 'MX' THEN x1.total_cost*0.05
     ELSE x1.total_cost END total_cost_helper
, CASE WHEN team_spend = 'GROWTH SPEND' and x1.country = 'US' THEN
    CASE WHEN x1.platform_v2 = 'ROKU' AND x1.campaign_name LIKE '%ICS%' THEN total_cost
         WHEN (dpi.new_percentage != 0 OR dpi.returning_percentage != 0) AND dpi.new_percentage IS NOT NULL AND dpi.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(dpi.new_percentage/100)
         WHEN (nra.new_percentage != 0 OR nra.returning_percentage != 0) AND nra.new_percentage IS NOT NULL AND nra.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(nra.new_percentage/100)
         WHEN (bckp.new_percentage != 0 OR bckp.returning_percentage != 0) AND bckp.new_percentage IS NOT NULL AND bckp.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(bckp.new_percentage/100)
         WHEN (plt.new_percentage != 0 OR plt.returning_percentage != 0) AND plt.new_percentage IS NOT NULL AND plt.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(plt.new_percentage/100)
         END
END as new_cost
, CASE WHEN team_spend = 'GROWTH SPEND' and x1.country = 'US' THEN
    CASE WHEN x1.platform_v2 = 'ROKU' AND x1.campaign_name LIKE '%ICS%' THEN 0
         WHEN (dpi.new_percentage != 0 OR dpi.returning_percentage != 0) AND dpi.new_percentage IS NOT NULL AND dpi.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(dpi.returning_percentage/100)
         WHEN (nra.new_percentage != 0 OR nra.returning_percentage != 0) AND nra.new_percentage IS NOT NULL AND nra.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(nra.returning_percentage/100)
         WHEN (bckp.new_percentage != 0 OR bckp.returning_percentage != 0) AND bckp.new_percentage IS NOT NULL AND bckp.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(bckp.returning_percentage/100)
         WHEN (plt.new_percentage != 0 OR plt.returning_percentage != 0) AND plt.new_percentage IS NOT NULL AND plt.returning_percentage IS NOT NULL
                    THEN total_cost_helper*(plt.returning_percentage/100)
         END
END as returning_cost

, CASE WHEN team_spend = 'GROWTH SPEND' and x1.country = 'US' THEN
     CASE WHEN x1.platform_v2 = 'ROKU' AND x1.campaign_name LIKE '%ICS%' THEN 'ROKU ICS Logic'
         WHEN (dpi.new_percentage != 0 OR dpi.returning_percentage != 0) AND dpi.new_percentage IS NOT NULL AND dpi.returning_percentage IS NOT NULL
                    THEN 'DAILY IMPRESSIONS'
         WHEN (nra.new_percentage != 0 OR nra.returning_percentage != 0) AND nra.new_percentage IS NOT NULL AND nra.returning_percentage IS NOT NULL
                    THEN 'AVG CTG CT - CURRENT DAY'
         WHEN (bckp.new_percentage != 0 OR bckp.returning_percentage != 0) AND bckp.new_percentage IS NOT NULL AND bckp.returning_percentage IS NOT NULL
                    THEN '6M AVG PLATFORM CTG CT'
         WHEN (plt.new_percentage != 0 OR plt.returning_percentage != 0) AND plt.new_percentage IS NOT NULL AND plt.returning_percentage IS NOT NULL
                    THEN '6M AVG PLATFORM'
         END
END as spend_logic
, x1.video_views
, x1.completed_video_views
, x1.video_views_25pct
, x1.video_views_50pct
, x1.video_views_75pct
, x1.revenue_actual
, x1.original_revenue_actual
, x1.arpu_actual
, x1.roi_actual
, x1.start_video_cpe_actual
, x1.tvt_proxy_ecvr_actual
, 10min_played_cpe_actual
, x1.start_video_ecvr_actual
, 10min_played_ecvr_actual
, x1.tvt_proxy_cpe_actual
FROM
  preFinal_CTE x1

  LEFT JOIN (
    SELECT 
      DISTINCT
      content_id
    , FIRST_VALUE(program_id) IGNORE NULLS OVER(PARTITION BY content_id ORDER BY updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS program_id
    , FIRST_VALUE(content_type) IGNORE NULLS OVER(PARTITION BY content_id ORDER BY updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS content_type
    , FIRST_VALUE(program_name) IGNORE NULLS OVER(PARTITION BY content_id ORDER BY updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS program_name
    FROM
      tubidw.content_info
    ) x2
  ON TRIM(COALESCE(x1.campaign_content_id, x1.campaign_adgroup_content_id)) = TRIM(x2.content_id)

  LEFT JOIN (
    SELECT 
      DISTINCT
      program_id
    , FIRST_VALUE(content_type) IGNORE NULLS OVER(PARTITION BY program_id ORDER BY updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS content_type
    , FIRST_VALUE(program_name) IGNORE NULLS OVER(PARTITION BY program_id ORDER BY updated_at DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS program_name
    FROM
      tubidw.content_info
    ) x3
  ON TRIM(COALESCE(x1.campaign_content_id, x1.campaign_adgroup_content_id)) = TRIM(x3.program_id)
  LEFT JOIN temp.enriched_campaign_retargeting ect
  ON 
    x1.campaign_id = ect.cmp_id 
    AND x1.platform_V2 = ect.platform
    AND x1.ds BETWEEN ect.start_date AND ect.end_date
  LEFT JOIN temp.tubi_originals_content toc
   ON NVL(x1.campaign_content_id, x1.campaign_adgroup_content_id) = toc.ids
  LEFT JOIN temp.content_top_2_genre top2
   ON NVL(x1.campaign_content_id, x1.campaign_adgroup_content_id) = top2.ids
  LEFT JOIN temp.growth_daily_platform_impressions dpi
      ON x1.ds = dpi.created_date
          AND trim(x1.platform_V2) = trim(dpi.platform)
          AND upper(trim(x1.campaign_id)) = upper(trim(dpi.campaign_id))
          AND upper(trim(x1.campaign_targeting_group)) = upper(trim(dpi.campaign_targeting_group))
          AND upper(trim(x1.campaign_type)) = upper(trim(dpi.campaign_type))
  LEFT JOIN temp.new_returning_average_platform nra 
      ON x1.ds = nra.attribution_date
          AND trim(x1.platform_V2) = trim(nra.platform)
          AND upper(trim(x1.campaign_targeting_group)) = upper(trim(nra.campaign_targeting_group))
          AND upper(trim(x1.campaign_type)) = upper(trim(nra.campaign_type))
  LEFT JOIN temp.growth_new_returning_percentage_backupavg bckp
     ON trim(x1.platform_V2) = trim(bckp.platform)
          AND upper(trim(x1.campaign_targeting_group)) = upper(trim(bckp.campaign_targeting_group))
          AND upper(trim(x1.campaign_type)) = upper(trim(bckp.campaign_type))
  LEFT JOIN temp.growth_avg_platform plt
     ON trim(x1.platform_V2) = trim(plt.platform)

