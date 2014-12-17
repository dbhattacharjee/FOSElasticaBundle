<?php

namespace FOS\ElasticaBundle\Elastica;

use Elastica\Client as BaseClient;
use Elastica\Request;
use FOS\ElasticaBundle\Logger\ElasticaLogger;
use Symfony\Component\Stopwatch\Stopwatch;

/**
 * Extends the default Elastica client to provide logging for errors that occur
 * during communication with ElasticSearch.
 *
 * @author Gordon Franke <info@nevalon.de>
 */
class Client extends BaseClient
{
    /**
     * Stores created indexes to avoid recreation.
     *
     * @var array
     */
    private $indexCache = array();

    /**
     * Symfony's debugging Stopwatch
     *
     * @var Stopwatch|null
     */
    private $stopwatch;
    
     /**
     *
     * @object Request
     */
    private $request;
    
    /**
     * Creates a new Elastica client
     *
     * @param array    $config   OPTIONAL Additional config options
     * @param callback $callback OPTIONAL Callback function which can be used to be notified about errors (for example connection down)
     * @param object   $containerObj  Request object
     */
    public function __construct(array $config = array(), $callback = null, $containerObj = null)
    {
        parent::__construct($config, $callback);
        try {
            $this->request = $containerObj->get('request');
        } catch (\Exception $e) {
            //fail silently as this would not work during cache clear
        }
    }

    /**
     * {@inheritdoc}
     */
    public function request($path, $method = Request::GET, $data = array(), array $query = array())
    {
        if ($this->stopwatch) {
            $this->stopwatch->start('es_request', 'fos_elastica');
        }
        //Hack : Replace index name with current subdomain
        if ($this->request->getSession()->get('_current_subdomain', false) && is_string($data)) {
            $data = str_replace('replaceable_index_name', $this->request->getSession()->get('_current_subdomain'), $data);
        }
        $start = microtime(true);
        $response = parent::request($path, $method, $data, $query);

        if ($this->_logger and $this->_logger instanceof ElasticaLogger) {
            $time = microtime(true) - $start;

            $connection = $this->getLastRequest()->getConnection();

            $connection_array = array(
                'host'      => $connection->getHost(),
                'port'      => $connection->getPort(),
                'transport' => $connection->getTransport(),
                'headers'   => $connection->hasConfig('headers') ? $connection->getConfig('headers') : array(),
            );

            $this->_logger->logQuery($path, $method, $data, $time, $connection_array, $query);
        }

        if ($this->stopwatch) {
            $this->stopwatch->stop('es_request');
        }

        return $response;
    }

    public function getIndex($name)
    {
        if (isset($this->indexCache[$name])) {
            return $this->indexCache[$name];
        }

        return $this->indexCache[$name] = new Index($this, $name);
    }

    /**
     * Sets a stopwatch instance for debugging purposes.
     *
     * @param Stopwatch $stopwatch
     */
    public function setStopwatch(Stopwatch $stopwatch = null)
    {
        $this->stopwatch = $stopwatch;
    }
}
