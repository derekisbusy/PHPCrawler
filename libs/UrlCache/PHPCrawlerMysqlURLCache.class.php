<?php

/**
 * Class for caching/storing URLs/links in a MySQL database.
 *
 * @package phpcrawl
 * @internal
 */
class PHPCrawlerMysqlURLCache extends PHPCrawlerURLCacheBase {

    protected $db_analyzed = false;

    public function __construct() {
    }

    public function getUrlCount() {

        return $wpdb->get_var("SELECT COUNT(*) FROM {$wpdb->prefix}crawler_urls WHERE processed = 0");
    }

    /**
     * Returns the next URL from the cache that should be crawled.
     *
     * @return PhpCrawlerURLDescriptor An PhpCrawlerURLDescriptor or NULL if currently no
     *                                 URL to process.
     */
    public function getNextUrl() {
        PHPCrawlerBenchmark::start("fetching_next_url_from_mysqlcache");
        $wpdb->query('START TRANSACTION');
        $ok = $this->PDO->exec("BEGIN EXCLUSIVE TRANSACTION");

        // Get row with max priority-level
        $Result = $this->PDO->query("SELECT max(priority_level) AS max_priority_level FROM urls WHERE in_process = 0 AND processed = 0;");
        $row = $Result->fetch(PDO::FETCH_ASSOC);

        $max_priority_level = $wpdb->get_var("SELECT max(priority_level) as max_priority_level FROM {$wpdb->prefix}crawler_urls WHERE in_process = 0 AND processed = 0");


        if ($max_priority_level == null) {
            $wpdb->query('COMMIT');
            return null;
        }
        $row = $wpdb->get_row("SELECT * FROM {$wpdb->prerfix}crawler_urls WHERE priority_level = " . $row["max_priority_level"] . " and in_process = 0 AND processed = 0", ARRAY_A);
        
        $wpdb->query("UPDATE {$wpdb->prefix}crawler_urls SET in_process = 1 WHERE id = {$row['id']}");
        $wpdb->query('COMMIT');

        // Update row (set in process-flag)
        $this->PDO->exec("UPDATE urls SET in_process = 1 WHERE id = " . $row["id"] . ";");

        $this->PDO->exec("COMMIT;");

        PHPCrawlerBenchmark::stop("fetching_next_url_from_mysqlcache");

        // Return URL
        return new PHPCrawlerURLDescriptor($row["url_rebuild"], $row["link_raw"], $row["link_code"], $row["link_text"], $row["refering_url"], $row["url_link_depth"]);
    }

    /**
     * Has no function in this class
     */
    public function getAllURLs() {
        
    }

    /**
     * Removes all URLs and all priority-rules from the URL-cache.
     */
    public function clear() {
        $wpdb->query("DELETE FROM {$wpdb->prefix}crawler_urls WHERE crawler_id = 1");
    }

    /**
     * Adds an URL to the url-cache
     *
     * @param PHPCrawlerURLDescriptor $UrlDescriptor      
     */
    public function addURL(PHPCrawlerURLDescriptor $UrlDescriptor) {
        if ($UrlDescriptor == null)
            return;

        // Hash of the URL
        $map_key = md5($UrlDescriptor->url_rebuild);

        // Get priority of URL
        $priority_level = $this->getUrlPriority($UrlDescriptor->url_rebuild);

        $wpdb->insert("{$wpdb->prefix}crawler_urls",
                [
                    ":priority_level" => $priority_level,
                    ":distinct_hash" => $map_key,
                    ":link_raw" => $UrlDescriptor->link_raw,
                    ":linkcode" => $UrlDescriptor->linkcode,
                    ":linktext" => $UrlDescriptor->linktext,
                    ":refering_url" => $UrlDescriptor->refering_url,
                    ":url_rebuild" => $UrlDescriptor->url_rebuild,
                    ":is_redirect_url" => $UrlDescriptor->is_redirect_url,
                    ":url_link_depth" => $UrlDescriptor->url_link_depth
                ],
                [
                   '%d','%s','%s','%s','%s','%s','%s','%s','%d'
                ]);
    }

    /**
     * Adds an bunch of URLs to the url-cache
     *
     * @param array $urls  A numeric array containing the URLs as PHPCrawlerURLDescriptor-objects
     */
    public function addURLs($urls) {
        PHPCrawlerBenchmark::start("adding_urls_to_mysqlcache");

        $wpdb->query('START TRANSACTION');

        $cnt = count($urls);
        for ($x = 0; $x < $cnt; $x++) {
            if ($urls[$x] != null) {
                $this->addURL($urls[$x]);
            }

            // Commit after 1000 URLs (reduces memory-usage)
            if ($x % 1000 == 0 && $x > 0) {
                $wpdb->query('COMMIT');
                $wpdb->query('START TRANSACTION');
            }
        }

        $wpdb->query('COMMIT');
        $this->PreparedInsertStatement->closeCursor();

        if ($this->db_analyzed == false) {
            $wpdb->query('ANALYZE TABLE {$wpdb->prefix}crawler_urls');
            $this->db_analyzed = true;
        }

        PHPCrawlerBenchmark::stop("adding_urls_to_mysqlcache");
    }

    /**
     * Marks the given URL in the cache as "followed"
     *
     * @param PHPCrawlerURLDescriptor $UrlDescriptor
     */
    public function markUrlAsFollowed(PHPCrawlerURLDescriptor $UrlDescriptor) {
        PHPCrawlerBenchmark::start("marking_url_as_followes");
        $hash = md5($UrlDescriptor->url_rebuild);
        $wpdb->query("UPDATE {$wpdb->prefix}crawler_urls SET processed = 1, in_process = 0 WHERE distinct_hash = '{$hash}'");
        PHPCrawlerBenchmark::stop("marking_url_as_followes");
    }

    /**
     * Checks whether there are URLs left in the cache that should be processed or not.
     *
     * @return bool
     */
    public function containsURLs() {
        PHPCrawlerBenchmark::start("checking_for_urls_in_cache");

        $row = $wpdb->get_row("SELECT id FROM {$wpdb->prefix}crawler_urls WHERE processed = 0 OR in_process = 1 LIMIT 1", ARRAY_N);

        PHPCrawlerBenchmark::stop("checking_for_urls_in_cache");

        if ($row !== null) {
            return true;
        }
        return false;
    }

    /**
     * Cleans/purges the URL-cache from inconsistent entries.
     */
    public function purgeCache() {
        // Set "in_process" to 0 for all URLs
        $wpdb->query("UPDATE {$wpdb->prefix}crawler_urls SET in_process = 0");
    }



    /**
     * Cleans up the cache after is it not needed anymore.
     */
    public function cleanup() {
        
    }

}

?>