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

class Danslo_ApiImport_Model_Log_ShellAdapter extends Mage_Core_Model_Log_Adapter {
    private $hadOutput = false;

    /**
     * Danslo_ApiImport_Model_Log_ShellAdapter constructor.
     */
    public function __construct()
    {
        parent::__construct("");
    }


    public function log($data = null)
    {
        $this->hadOutput = true;
        if (is_array($data)) {
            foreach ($data as $line) {
                if (is_string($line)) {
                    echo $line . "\n";
                } else {
                    echo rtrim(print_r($line, true)) . "\n";
                }
            }
            return;
        }
        echo rtrim($data) . "\n";
    }

    public function isHadOutput()
    {
        return $this->hadOutput;
    }
}
