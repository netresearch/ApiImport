<?php
/*
 * Copyright 2019 Netresearch DTT GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

require_once dirname($_SERVER['SCRIPT_FILENAME']) . '/abstract.php';

class Danslo_ApiImport_Shell extends Mage_Shell_Abstract {
    /**
     * @var Danslo_ApiImport_Model_Import_Api
     */
    private $api;

    /**
     * @var Danslo_ApiImport_Model_Log_ShellAdapter
     */
    private $logger;

    private $types = [
        'categories' => ['importEntities', 'catalog_category'],
        'products' => ['importEntities', 'catalog_product'],
        'customers' => ['importEntities', 'catalog_customer']
    ];

    private $behaviour = 'replace';
    private $delimiter = ";";
    private $enclosure = '"';
    private $escapeChar = '"';
    private $indexes = false;

    private $file;
    private $type;

    public function run()
    {
        $handle = fopen($this->file, 'r');
        $entities = array();
        $header = $this->readLine($handle);
        $count = 0;
        while ($row = $this->readLine($handle)) {
            $entities[] = array_filter(array_combine($header, $row), function ($cell) { return $cell !== null && $cell !== ""; });
            $count++;
        }

        $arguments = $this->types[$this->type];
        $method = array_shift($arguments);
        array_unshift($arguments, $entities);
        array_push($arguments, $this->behaviour);

        try {
            call_user_func_array([$this->api, $method], $arguments);
            if (!$this->logger->isHadOutput()) {
                echo "Imported $count $this->type\n";
            }
        } catch (Mage_Api_Exception $e) {
            echo $e->getMessage() . ":\n";
            echo $e->getCustomMessage() . "\n";
            exit(1);
        }

        if ($this->indexes) {
            $_SERVER['argv'] = ($this->indexes === 'all') ? ['reindexall'] : ['--reindex', $this->indexes];
            require_once __DIR__ . '/indexer.php';
        }
    }

    private function readLine($handle)
    {
        return fgetcsv($handle, 0, $this->delimiter, $this->enclosure, $this->escapeChar);
    }

    private function setupApi()
    {
        $this->logger = $this->_factory->getModel('api_import/log_shellAdapter');
        $this->api = $this->_factory->getModel('api_import/import_api');
        $this->api->getImportApi()->setLogger($this->logger);

        $refClass = new ReflectionObject($this->api);
        foreach ($refClass->getMethods(ReflectionMethod::IS_PUBLIC) as $method) {
            $name = $method->getName();
            if (substr($name, 0, 6) === 'import' && $name !== 'importEntities') {
                // CamelCase to camel-case
                $type = strtolower(preg_replace("/([A-Z])/", "-$1", lcfirst(substr($name, 6))));
                $this->types[$type] = [ $name ];
            }
        }
    }

    protected function _parseArgs()
    {
        $this->setupApi();

        $args = $_SERVER['argv'];
        $error = false;
        array_shift($args); // Shift off the script name
        while ($args) {
            $next = array_shift($args);
            if ($next === '-h' || $next === '--help') {
                $this->_args['help'] = true;
                return;
            } elseif ($next[0] === '-') {
                if ($next === '-b') {
                    $this->behaviour = array_shift($args);
                } elseif ($next === '-d') {
                    $this->delimiter = array_shift($args);
                } elseif ($next === '-e') {
                    $this->enclosure = array_shift($args);
                } elseif ($next === '-s') {
                    $this->escapeChar = array_shift($args);
                } elseif ($next === '-i') {
                    $this->indexes = array_shift($args);
                } elseif ($next === '-c') {
                    if (!@chdir($dir = array_shift($args))) {
                        $error = "Could not change to $dir";
                    }
                } else {
                    $error = "Invalid option $next";
                    break;
                }
            } else {
                array_unshift($args, $next);
                break;
            }
        }

        $this->file = array_pop($args);
        $this->type = array_pop($args);

        if (!$this->file || !$this->type) {
            $error = 'To few arguments';
        } elseif (count($args)) {
            $error = 'To many arguments';
        } elseif (!file_exists($this->file) || !is_readable($this->file)) {
            $error = "File '$this->file' does not exists or isn't readable";
        } elseif (!array_key_exists($this->type, $this->types)) {
            $error = "Invalid type '$this->type'";
        } elseif (
            !in_array($this->behaviour, ['replace', 'append', 'delete'], true)
            && !(substr($this->type, 0, 9) === 'attribute' && $this->behaviour === 'delete_if_not_exist')
        ) {
            $error = "Invalid behaviour $this->behaviour";
        }

        if ($error) {
            echo $error . "\n";
            exit(1);
        }
    }


    public function usageHelp()
    {
        $indexes = [];
        /* @var Mage_Index_Model_Indexer $indexer */
        $indexer = $this->_factory->getSingleton($this->_factory->getIndexClassAlias());
        foreach ($indexer->getProcessesCollection() as $process) {
            if ($process->getIndexer()->isVisible() !== false) {
                $indexes[] = $process->getIndexerCode();
            }
        }

        return implode("\n", [
            $_SERVER['SCRIPT_FILENAME'] . " [OPTIONS] TYPE CSV_FILE\n",
            "Invokes the import of ApiImport according to TYPE for the CSV_FILE\n",
            "Arguments:",
            "TYPE      Type of the import entities - possible types are: ",
            "            " . implode("\n            ", array_keys($this->types)),
            "CSV_FILE  Path to the CSV file containing the entities\n",
            "Options:",
            "-b BEHAVIOUR  Behaviour  - can be 'append', 'replace' or 'delete' and ",
            "              (only for the attribute*-types) 'delete_if_not_exist'",
            "              Defaults to 'replace'",
            "-d DELIMITER  The csv delimiter (defaults to ;)",
            "-e ENCLOSURE  Text enclosure (defaults to \"",
            "-s ESCAPE     Escape char (defaults to \\)",
            "-i INDEXES    Rebuild the given (comma separated) or all indexes - ",
            "              valid indexes are:",
            "                all,",
            "                " . implode(",\n                ", $indexes),
            "-c PATH       Change dir to PATH prior to the import (root path of a",
            "              magento installation)\n"
        ]);
    }
}

$shell = new Danslo_ApiImport_Shell();
$shell->run();
