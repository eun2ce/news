| field          | type        | 설명                                                           |
| -------------- | ----------- | ------------------------------------------------------------ |
| `id`           | string      | 제목+URL 해시(16자)                                               |
| `source_name`  | string      | 언론사/피드명                                                      |
| `source_url`   | string      | RSS 원본 URL                                                   |
| `url`          | string      | 기사 원문 URL                                                    |
| `title`        | string      | 기사 제목                                                        |
| `summary`      | string      | RSS 요약/설명(태그 제거)                                             |
| `content`      | string|null | (옵션) 본문 전체. `FETCH_FULLTEXT=true`일 때만                        |
| `category`     | string      | `politics/economy/society/culture/it_science/sports/general` |
| `tags`         | array[str]  | `breaking/analysis/opinion/short_title` 등 간단 태그              |
| `language`     | string      | 기본 `"ko"`                                                    |
| `published_at` | ISO string  | 기사 시각(KST로 변환)                                               |
| `fetched_at`   | ISO string  | 수집 시각(KST)                                                   |

