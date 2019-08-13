<?php
/*
 * Copyright 2011 Daniel Sloof
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

class Danslo_ApiImport_Model_Import_Api
    extends Mage_Api_Model_Resource_Abstract
{

    /**
     * Cached import model.
     *
     * @var Danslo_ApiImport_Model_Import
     */
    protected $_api;

    /**
     * @var Mage_Catalog_Model_Resource_Eav_Mysql4_Setup
     */
    protected $_setup;

    /**
     * @var int
     */
    protected $_catalogProductEntityTypeId;

    /**
     * @var array
     */
    protected $_storeCodeToId;

    /**
     * Sets up the import model and loads area parts.
     *
     * @return void
     */
    public function __construct()
    {
        $this->_api = Mage::getModel('api_import/import');

        // Event part is not loaded by default for API.
        Mage::app()->loadAreaPart(Mage_Core_Model_App_Area::AREA_GLOBAL, Mage_Core_Model_App_Area::PART_EVENTS);
    }

    /**
     * @return Danslo_ApiImport_Model_Import
     */
    public function getImportApi() {
        return $this->_api;
    }

    /**
     * Fires off the import process through the import model.
     *
     * @param array $entities
     * @param string $entityType
     * @param string $behavior
     * @return array
     */
    public function importEntities($entities, $entityType = null, $behavior = null)
    {
        $this->_setEntityTypeCode($entityType ? $entityType : Mage_Catalog_Model_Product::ENTITY);
        $this->_setBehavior($behavior ? $behavior : Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE);

        $this->_api->getDataSourceModel()->setEntities($entities);
        try {
            $result = $this->_api->importSource();
            $errorsCount = $this->_api->getErrorsCount();
            if ($errorsCount > 0) {
                Mage::throwException("There were {$errorsCount} errors during the import process." .
                    "Please be aware that valid entities were still imported.");
            };
        } catch(Mage_Core_Exception $e) {
            $this->_fault('import_failed', $e->getMessage());
        }

        return array($result);
    }

    /**
     * Import attributes and put them in attribute sets and attribute group
     *
     * @param array  $data
     * @param string $behavior
     *
     * @return true
     */
    public function importAttributes(array $data, $behavior = null)
    {
        if (null === $behavior) {
            $behavior = Mage_ImportExport_Model_Import::BEHAVIOR_APPEND;
        }
        $this->_init();
        /** @var Mage_Eav_Model_Config $config */
        $config = Mage::getSingleton('eav/config');

        if (Danslo_ApiImport_Model_Import::BEHAVIOR_DELETE_IF_NOT_EXIST === $behavior) {
            $this->_pruneAttributes($data);
        } else {
            foreach ($data as $attribute) {
                if (isset($attribute['attribute_id'])) {
                    $attributeCode = $attribute['attribute_id'];
                    unset($attribute['attribute_id']);

                    if (Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE === $behavior
                        || Mage_ImportExport_Model_Import::BEHAVIOR_APPEND === $behavior) {
                        $labels = $this->extractStoreFields($attribute, 'label');
                        $this->_setup->addAttribute($this->_catalogProductEntityTypeId, $attributeCode, $attribute);
                        if ($labels) {
                            try {
                                $attributeId = $this->_setup->getAttributeId($this->_catalogProductEntityTypeId, $attributeCode);
                                $config->getAttribute($this->_catalogProductEntityTypeId, $attributeId)
                                    ->setData('store_labels', $labels)
                                    ->save();
                            } catch (Exception $exception) {
                                $this->_api->addLogComment("Could not update labels for " . $attributeCode);
                            }
                        }
                    } elseif (Mage_ImportExport_Model_Import::BEHAVIOR_DELETE === $behavior) {
                        $this->_setup->removeAttribute($this->_catalogProductEntityTypeId, $attributeCode);
                    }
                }
            }
        }

        return true;
    }

    public function importAttributeOptions(array $data, $behavior = Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE) {
        $this->_init();
        $attributeData = [];
        foreach ($data as $i => $row) {
            if (!is_array($row)) {
                $this->_api->addLogComment("[ERROR] Invalid row $i");
                continue;
            }
            if (empty($row['attribute_id'])) {
                $this->_api->addLogComment("[ERROR] Invalid row $i: Missing attribute_id");
                continue;
            }
            $attributeCode = $row['attribute_id'];
            $labels = $this->extractStoreFields($row, 'label');
            if (array_key_exists('label', $row) && !array_key_exists(0, $labels)) {
                $labels[0] = $row['label'];
            }
            if (!$labels) {
                $this->_api->addLogComment("[ERROR] Invalid row $i: No label(s)");
                continue;
            }
            if (!array_key_exists($attributeCode, $attributeData)) {
                $attributeData[$attributeCode] = [];
            }
            $attributeData[$attributeCode][] = $labels;
        }
        foreach ($attributeData as $attributeCode => $attributeOptions) {
            /** @var Mage_Catalog_Model_Entity_Attribute $attribute */
            $attribute = Mage::getModel('eav/entity_attribute');
            $attribute->loadByCode('catalog_product', $attributeCode);
            if (!$attribute->getId()) {
                $this->_api->addLogComment("[ERROR] No attribute with code $attributeCode");
                continue;
            }
            if (!in_array($attribute->getFrontendInput(), ['select', 'multiselect'])) {
                $this->_api->addLogComment("[ERROR] Attribute $attributeCode is not a select/multiselect");
                continue;
            }

            /** @var Mage_Eav_Model_Entity_Attribute_Source_Table $source */
            $source = $attribute->getSource();
            $present = [];
            foreach (array_merge([0], $this->_storeCodeToId) as $storeId) {
                $attribute->setStoreId($storeId);
                $presentOptions = $source->getAllOptions(false);
                foreach ($presentOptions as $presentOption) {
                    $optionId = $presentOption['value'];
                    $optionLabel = $presentOption['label'];
                    if (!array_key_exists($optionId, $present)) {
                        $present[$optionId] = [];
                    }
                    $present[$presentOption['value']][$storeId] = $presentOption['label'];
                }
            }
            $options = ['delete' => [], 'value' => [], 'attribute_id' => $attribute->getId()];
            foreach ($attributeOptions as $i => $labels) {
                foreach ($present as $optionId => $presentLabels) {
                    if (!array_diff_key($labels, $presentLabels)) {
                        // Found option
                        if ($behavior === Mage_ImportExport_Model_Import::BEHAVIOR_DELETE) {
                            $options['delete'][$optionId] = true;
                            $options['value'][$optionId] = $presentLabels;
                        }
                        unset($present[$optionId]);
                        unset($attributeOptions[$i]);
                        continue 2;
                    }
                }
            }
            if ($behavior === Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE) {
                foreach ($present as $optionId => $labels) {
                    $options['delete'][$optionId] = true;
                    $options['value'][$optionId] = $labels;
                }
            }
            if ($behavior === Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE || $behavior === Mage_ImportExport_Model_Import::BEHAVIOR_APPEND) {
                foreach ($attributeOptions as $i => $labels) {
                    $options['value']['new_' . $i] = $labels;
                }
            }
            if (count($options['value'])) {
                $this->_setup->addAttributeOption($options);
            }
        }
        return true;
    }

    private function extractStoreFields(array &$row, $field) {
        $labels = [];
        foreach ($this->_storeCodeToId as $storeCode => $storeId) {
            $key = $field . '-' . $storeCode;
            if (array_key_exists($key, $row)) {
                $labels[$storeId] = $row[$key];
                unset($row[$key]);
            }
        }
        return $labels;
    }

    /**
     * Compute attributes sets and their groups and import them
     *
     * @param array  $data
     * @param string $behavior
     *
     * @return true
     */
    public function importAttributeSets(array $data, $behavior = null)
    {
        if (null === $behavior) {
            $behavior = Mage_ImportExport_Model_Import::BEHAVIOR_APPEND;
        }
        $this->_init();

        if (Mage_ImportExport_Model_Import::BEHAVIOR_DELETE === $behavior) {
            $this->_removeAttributeSets($data);
        } elseif (Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE === $behavior
            || Mage_ImportExport_Model_Import::BEHAVIOR_APPEND === $behavior) {
            $this->_updateAttributeSets($data);
        } elseif (Danslo_ApiImport_Model_Import::BEHAVIOR_DELETE_IF_NOT_EXIST === $behavior) {
            $this->_pruneAttributeSets($data);
        }

        return true;
    }

    /**
     * Links attributes to attributes group and attribute sets
     *
     * @param array  $data
     * @param string $behavior
     *
     * @return bool
     */
    public function importAttributeAssociations(array $data, $behavior = null)
    {
        if (null === $behavior) {
            $behavior = Mage_ImportExport_Model_Import::BEHAVIOR_APPEND;
        }
        $this->_init();

        if (Mage_ImportExport_Model_Import::BEHAVIOR_DELETE === $behavior) {
            $this->_removeAttributeFromGroup($data);
        } elseif (Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE === $behavior
            || Mage_ImportExport_Model_Import::BEHAVIOR_APPEND === $behavior) {
            $this->_updateAttributeAssociations($data);
        } elseif (Danslo_ApiImport_Model_Import::BEHAVIOR_DELETE_IF_NOT_EXIST === $behavior) {
            $this->_pruneAttributesFromAttributeSets($data);
        }

        return true;
    }

    /**
     * Get potential true string values a true, others as false
     *
     * @param string $str
     * @return bool
     */
    private function strToBool($str) {
        return in_array($str, ['true', '1', 1, true, 'yes'], true);
    }

    /**
     * Detect ambiguously set values (e.g. one row says website with code base has
     * name "name1" and others say it has name "name2")
     *
     * @param array $keys
     * @param array $new
     * @param array $existing
     * @param string $type
     * @param int $i
     * @return bool
     */
    private function hasAmbiguousValues(array $keys, array $new, array $existing, $type, $i) {
        $ambiguous = false;
        foreach ($keys as $key) {
            $existingValue = $existing[$key];
            $newValue = $new[$type . '_' . $key];
            if (is_bool($existingValue)) {
                $newValue = $this->strToBool($newValue);
            }
            if ($newValue !== $existingValue) {
                $ambiguous = true;
                $this->_api->addLogComment(
                    "[ERROR] Invalid row $i: Conflict for {$type}_$key - already registered with a different value"
                );
            }
        }
        return $ambiguous;
    }

    /**
     * Detect if already added records were marked as default and reset
     * is_default on $new in this case
     *
     * @param string $type
     * @param string $id
     * @param array $new
     * @param array[] $existings
     * @param int $i
     */
    private function fixConflictWithOtherDefaults($type, $id, &$new, $existings, $i) {
        if (!$new['is_default']) {
            return;
        }
        foreach ($existings as $existingId => $existing) {
            if ($existingId !== $id && $existing['is_default']) {
                $this->_api->addLogComment(
                    "[WARNING] Default $type conflict in line $i - keeping '$existingId'"
                );
                $new['is_default'] = false;
            }
        }
    }

    /**
     * Get a hierarchical representation of the flat store rows
     *
     * @param array $data
     * @return array
     */
    protected function getWebsites($data) {
        $websites = [];
        $requiredKeys = ['code', 'name', 'is_default', 'group_name', 'group_root_category', 'group_is_default', 'website_code', 'website_name', 'website_is_default'];
        $defaults = ['is_active' => '0', 'sort_order' => '0'];
        foreach ($data as $i => $row) {
            $missing = [];
            foreach ($requiredKeys as $key) {
                if (empty($row[$key]) && $row[$key] !== "0") {
                    $missing[] = $key;
                }
            }
            if ($missing) {
                $this->_api->addLogComment("Invalid row $i: Missing columns " . implode(", ", $missing));
                continue;
            }
            if (!array_key_exists($wsCode = $row['website_code'], $websites)) {
                $websites[$wsCode] = [
                    'name' => $row['website_name'],
                    'groups' => [],
                    'is_default' => $this->strToBool($row['website_is_default'])
                ];
            }
            $website = &$websites[$wsCode];
            if (!$this->hasAmbiguousValues(['name', 'is_default'], $row, $website, 'website', $i)) {
                $this->fixConflictWithOtherDefaults('website', $wsCode, $website, $websites, $i);

                if (!array_key_exists($groupName = $row['group_name'], $website['groups'])) {
                    $website['groups'][$groupName] = [
                        'root_category' => $row['group_root_category'],
                        'stores' => [],
                        'is_default' => $this->strToBool($row['group_is_default'])
                    ];
                }
                $group = &$website['groups'][$groupName];
                if (!$this->hasAmbiguousValues(['root_category', 'is_default'], $row, $group, 'group', $i)) {
                    $this->fixConflictWithOtherDefaults('group', $groupName, $group, $website['groups'], $i);

                    $row = array_merge($defaults, $row);
                    $row['is_default'] = $this->strToBool($row['is_default']);
                    $row['is_active'] = $this->strToBool($row['is_active']);
                    $this->fixConflictWithOtherDefaults('store', $row['code'], $row, $group['stores'], $i);
                    $store = ['config' => [], 'index' => $i];
                    foreach ($row as $column => $cell) {
                        if (in_array($column, $requiredKeys) || array_key_exists($column, $defaults)) {
                            $store[$column] = $cell;
                        } else {
                            $store['config'][$column] = $cell;
                        }
                    }
                    $group['stores'][$row['code']] = $store;
                }
            }
        }
        return $websites;
    }

    /**
     * Import stores
     *
     * Currently there is "only" a full sync behaviour
     *
     * @param array $data
     * @param string $behavior
     * @return bool
     */
    public function importStores(array $data, $behavior = null) {
        if (null === $behavior) {
            $behavior = Mage_ImportExport_Model_Import::BEHAVIOR_APPEND;
        } elseif (!in_array($behavior, [Mage_ImportExport_Model_Import::BEHAVIOR_APPEND, Mage_ImportExport_Model_Import::BEHAVIOR_REPLACE])) {
            $this->_api->addLogComment("Behavior $behavior currently not supported");
            return false;
        }

        /* @var Mage_Core_Model_Website[] $existingWebsites */
        $existingWebsites = Mage::app()->getWebsites(false, true);
        /* @var Mage_Core_Model_Store_Group[] $existingGroups */
        $existingGroups = Mage::app()->getGroups();
        /* @var Mage_Core_Model_Store[] $existingStores */
        $existingStores = Mage::app()->getStores(false, true);

        $rootCategoryIds = [];
        /* @var Mage_Catalog_Model_Resource_Eav_Mysql4_Category_Collection $categories */
        $categories = Mage::getModel('catalog/category')->getCollection();
        $categories
            ->addNameToResult()
            ->setStoreId(0)
            ->addAttributeToFilter('level', 1);
        foreach ($categories as $category) {
            /* @var Mage_Catalog_Model_Category $category */
            $categoryName = $category->getName();
            if (array_key_exists($categoryName, $rootCategoryIds)) {
                $this->_api->addLogComment("[WARNING] Duplicate root category name: $categoryName");
            } else {
                $rootCategoryIds[$categoryName] = $category->getId();
            }
        }

        foreach ($this->getWebsites($data) as $wsCode => $wsData) {
            if (!array_key_exists($wsCode, $existingWebsites)) {
                $this->_api->addLogComment("[INFO] Adding new website '$wsCode'");
                $existingWebsites[$wsCode] = Mage::getModel('core/website')->setCode($wsCode);
            }
            $website = $existingWebsites[$wsCode];
            if ($wsData['is_default'] && !$website->getIsDefault()) {
                $this->_api->addLogComment("[INFO] Setting website '$wsCode' as default");
                foreach ($existingWebsites as $existingWebsiteCode => $existingWebsite) {
                    if ($existingWebsiteCode !== $wsCode) {
                        $existingWebsite->setIsDefault(0);
                    }
                }
            }
            $website
                ->setIsDefault($wsData['is_default'] ? 1 : 0)
                ->setName($wsData['name'])
                ->save();

            foreach ($wsData['groups'] as $groupName => $gData) {
                if (!array_key_exists($gData['root_category'], $rootCategoryIds)) {
                    $this->_api->addLogComment("[INFO] Adding new root category '{$gData['root_category']}'");
                    /* @var Mage_Catalog_Model_Category $newCategory */
                    $newCategory = Mage::getModel('catalog/category');
                    $parentId = Mage_Catalog_Model_Category::TREE_ROOT_ID;
                    $parentCategory = Mage::getModel('catalog/category')->load($parentId);
                    $newCategory
                        ->setStoreId(0)
                        ->setData('name', $gData['root_category'])
                        ->setData('url_key', $gData['root_category'] . '-catalog')
                        ->setData('display_mode', 'PRODUCTS')
                        ->setData('path', $parentCategory->getPath())
                        ->setData('is_active', 1)
                        ->setData('level', 1);
                    $newCategory->save();
                    $rootCategoryIds[$gData['root_category']] = $newCategory->getId();
                }
                $categoryId = $rootCategoryIds[$gData['root_category']];

                /* @var Mage_Core_Model_Store_Group $group */
                $group = null;
                foreach ($existingGroups as $existingGroup) {
                    if ($existingGroup->getWebsiteId() === $website->getId()) {
                        if ($existingGroup->getRootCategoryId() === $categoryId && $existingGroup->getName() === $groupName) {
                            $group = $existingGroup;
                            break;
                        }
                        if ($existingGroup->getName() === $groupName) {
                            $group = $existingGroup;
                            $this->_api->addLogComment("[INFO] Setting root category of group '$groupName' on website '$wsCode' '{$gData['root_category']}'"
                            );
                            $group->setRootCategoryId($categoryId)->save();
                            break;
                        }
                        if ($existingGroup->getRootCategoryId() === $categoryId) {
                            $group = $existingGroup;
                            $groupName = $group->getName();
                            $this->_api->addLogComment("[INFO] Changing name of group '$groupName' on website '$wsCode' to $groupName");
                            $group->setName($groupName)->save();
                            break;
                        }
                    }
                }
                if (!$group) {
                    $this->_api->addLogComment("[INFO] Adding store group {$groupName} to website $wsCode");
                    $group = Mage::getModel('core/store_group');
                    $group->setWebsiteId($website->getId())
                        ->setName($groupName)
                        ->setRootCategoryId($categoryId)
                        ->save();
                    $existingGroups[$group->getId()] = $group;
                }

                if ($gData['is_default'] && $website->getDefaultGroupId() !== $group->getId()) {
                    $this->_api->addLogComment("[INFO] Setting '$groupName' as default group on '$wsCode'");
                    $website->setDefaultGroupId($group->getId())->save();
                }
                foreach ($gData['stores'] as $code => $storeData) {
                    if (!array_key_exists($code, $existingStores)) {
                        $this->_api->addLogComment("[INFO] Adding new store '$code' on group '$groupName' on website '$wsCode'");
                        $existingStores[$code] = Mage::getModel('core/store')->setCode($code);
                    }
                    $store = $existingStores[$code];
                    $store
                        ->setName($storeData['name'])
                        ->setWebsiteId($website->getId())
                        ->setGroupId($group->getId())
                        ->setSortOrder(intval($storeData['sort_order']))
                        ->setIsActive($storeData['is_active'] ? 1 : 0)
                        ->save();
                    $config = Mage::getConfig();
                    foreach ($storeData['config'] as $configKey => $configValue) {
                        if ($configKey === 'general/locale/code') {
                            if ($locale = $this->importLocale($configValue)) {
                                $configValue = $locale;
                            }
                        }
                        $config->saveConfig($configKey, $configValue, 'stores', $store->getId());
                    }
                    if ($storeData['is_default'] && $group->getDefaultStoreId() !== $store->getId()) {
                        $this->_api->addLogComment("[INFO] Setting '$code' as default store on group '$groupName' on website '$wsCode'");
                        $group->setDefaultStoreId($store->getId())->save();
                    }
                }
                foreach ($existingStores as $code => $store) {
                    if ($store->getGroupId() === $group->getId() && !array_key_exists($code, $gData['stores']) && $store->getIsActive()) {
                        $this->_api->addLogComment("[INFO] Deactivating store '$code'");
                        $store->setIsActive(0)->save();
                    }
                }
            }
        }

        return true;
    }

    public function importLocales(array $data) {
        foreach ($data as $i => $row) {
            if (empty($row['code']) || !is_string($row['code'])) {
                $this->_api->addLogComment("[ERROR] Missing code on row $i");
                continue;
            }
            $this->importLocale($row['code']);
        }
    }

    private function importLocale($code) {
        $knownLanguagePacks = [
            'de_DE' => 'riconeitzel/German_LocalePack_de_DE/preview',
            'fr_FR' => 'MaWoScha/German_LocalePack_fr_FR/master',
        ];
        if (!preg_match('/^[a-z]{2,2}_[A-Z]{2,2}$/', $code)) {
            $this->_api->addLogComment("[ERROR] Invalid locale code $code");
            return false;
        }
        $canonicalCode = null;
        if (array_key_exists($code, $knownLanguagePacks)) {
            $canonicalCode = $code;
        } else {
            $nativeCode = substr($code, 0, 3) . strtoupper(substr($code, 0, 2));
            if ($nativeCode !== $code && array_key_exists($nativeCode, $knownLanguagePacks)) {
                $canonicalCode = $nativeCode;
                $this->_api->addLogComment("[INFO] No language pack found for $code but for $nativeCode - use this");
            }
        }
        if (!$canonicalCode) {
            $this->_api->addLogComment("[WARNING] Unknown language pack for locale $code");
            return false;
        }
        list($user, $repo, $branch) = explode("/", $knownLanguagePacks[$canonicalCode]);
        $rootDir = Mage::getBaseDir();
        $modmanDir = $rootDir . '/.modman';
        $packDir = $modmanDir . '/' . $repo;
        if (!file_exists($packDir)) {
            $tmpDir = "/tmp/{$repo}-{$branch}";
            $url = "https://github.com/$user/$repo/archive/$branch.tar.gz";
            $this->_api->addLogComment("[INFO] Installing $url");
            if (!file_exists($modmanDir) && !$this->exec('modman init', $rootDir)) {
                return false;
            } elseif (!$this->exec("curl -sL $url | tar -xz", '/tmp')) {
                return false;
            } elseif (!file_exists($tmpDir) || !is_dir($tmpDir)) {
                $this->_api->addLogComment("[ERRROR] Downloaded file not in expected directory $tmpDir");
                return false;
            } elseif (!$this->exec("mv $tmpDir $packDir")) {
                $this->exec("rm -rf $tmpDir");
                return false;
            } elseif (!$this->exec("modman deploy $repo", $rootDir)) {
                $this->_api->addLogComment("[ERRROR] Could not deploy $repo");
                if (!$this->exec("rm -rf $packDir")) {
                    $this->_api->addLogComment("[ERRROR] Could not remove $packDir");
                }
                return false;
            }
        }

        return $canonicalCode;
    }

    private function exec($command, $cwd = null) {
        if ($cwd) {
            $command = "cd $cwd; " . $command;
        }
        $output = [];
        $returnCode = 0;
        exec($command, $output, $returnCode);
        if ($returnCode > 0) {
            $this->_api->addLogComment("[ERROR] Failed command: " . $command);
            foreach ($output as $line) {
                $this->_api->addLogComment("        " . $line);
            }
            return false;
        }
        return true;
    }

    /**
     * Initialize parameters
     *
     * @return void
     */
    protected function _init()
    {
        $this->_setup = new Mage_Catalog_Model_Resource_Eav_Mysql4_Setup('catalog_product_attribute_set');
        $this->_catalogProductEntityTypeId = $this->_setup->getEntityTypeId(Mage_Catalog_Model_Product::ENTITY);

        foreach (Mage::app()->getStores() as $store) {
            /** @var Mage_Core_Model_Store $store */
            $this->_storeCodeToId[$store->getCode()] = $store->getId();
        }
    }

    /**
     * Remove attribute and group association in attribute sets
     *
     * @param array $data
     *
     * @return void
     */
    protected function _removeAttributeFromGroup(array $data)
    {
        $entityTypeId = $this->_catalogProductEntityTypeId;

        foreach ($data as $attribute) {
            $setId       = $this->_setup->getAttributeSetId($entityTypeId, $attribute['attribute_set_id']);
            $attributeId = $this->_setup->getAttributeId($entityTypeId, $attribute['attribute_id']);
            $groupId     = $this->_setup->getAttributeGroupId(
                $entityTypeId,
                $attribute['attribute_set_id'],
                $attribute['attribute_group_id']
            );

            $this->_setup->getConnection()->delete(
                $this->_setup->getTable('eav/entity_attribute'),
                array (
                    new Zend_Db_Expr('entity_type_id = ' . $entityTypeId),
                    new Zend_Db_Expr('attribute_set_id = ' . $setId),
                    new Zend_Db_Expr('attribute_id = ' . $attributeId),
                    new Zend_Db_Expr('attribute_group_id = ' . $groupId)
                )
            );
        }
    }

    /**
     * Remove given attributes if not exist in Magento
     *
     * @param array $data
     *
     * @return void
     */
    protected function _pruneAttributes(array $data)
    {
        $select = $this->_setup->getConnection()
            ->select()
            ->from($this->_setup->getTable('eav/attribute'))
            ->where('is_user_defined = 1');
        $magAttributes = $this->_setup->getConnection()->fetchAssoc($select);

        foreach ($magAttributes as $magAttribute) {

            $attributeFound = false;
            while ((list($key, $attribute) = each($data)) && $attributeFound === false) {
                if ($attribute['attribute_id'] === $magAttribute['attribute_code']) {
                    $attributeFound = true;
                }
            }
            reset($data);

            if (!$attributeFound) {
                $this->_setup->removeAttribute($this->_catalogProductEntityTypeId, $magAttribute['attribute_code']);
            }
        }
    }

    /**
     * Delete associations if they exist in magento but not in given data
     *
     * @param array $data
     *
     * @return array
     */
    protected function _pruneAttributesFromAttributeSets(array $data)
    {
        $entityTypeId = $this->_catalogProductEntityTypeId;
        $query = $this->_setup->getConnection()
            ->select()
            ->from($this->_setup->getTable('eav/entity_attribute'))
            ->where('entity_type_id = :entity_type_id');
        $bind = array('entity_type_id' => $this->_catalogProductEntityTypeId);

        $givenAssociations = array();
        foreach ($data as $attribute) {
            $setId = $this->_setup->getAttributeSetId($entityTypeId, $attribute['attribute_set_id']);
            $givenAssociations[] = array(
                'attribute_id'       => $this->_setup->getAttributeId($entityTypeId, $attribute['attribute_id']),
                'attribute_set_id'   => $setId,
                'attribute_group_id' => $this->_setup->getAttributeGroupId(
                    $entityTypeId,
                    $setId,
                    $attribute['attribute_group_id']
                )
            );

        }

        $deletedRows = array();
        foreach ($this->_setup->getConnection()->fetchAssoc($query, $bind) as $magAssociation) {
            $rowFound = false;
            while ((list($key, $association) = each($givenAssociations)) && $rowFound === false) {
                if ($association['attribute_id'] === $magAssociation['attribute_id']
                    && $association['attribute_set_id'] === $magAssociation['attribute_set_id']
                    && $association['attribute_group_id'] === $magAssociation['attribute_group_id']
                ) {
                    $rowFound = true;
                }
            }
            reset($givenAssociations);

            if (!$rowFound) {
                $deletedRows[$magAssociation['entity_attribute_id']] = $this->_setup->getConnection()
                    ->delete(
                        $this->_setup->getTable('eav/entity_attribute'),
                        new Zend_Db_Expr('entity_attribute_id = ' . $magAssociation['entity_attribute_id'])
                    );
            }
        }

        return $deletedRows;
    }

    /**
     * Update associations between attributes, attribute groups and attribute sets
     *
     * @param array $data
     *
     * @return void
     */
    protected function _updateAttributeAssociations(array $data)
    {
        foreach ($data as $attribute) {
            $this->_setup->addAttributeToGroup(
                $this->_catalogProductEntityTypeId,
                $attribute['attribute_set_id'],
                $attribute['attribute_group_id'],
                $attribute['attribute_id'],
                $attribute['sort_order']
            );
        }
    }

    /**
     * Remove attribute sets
     *
     * @param array $data
     *
     * @return void
     */
    protected function _removeAttributeSets(array $data)
    {
        foreach ($data as $attributeSet) {
            $this->_setup->removeAttributeSet('catalog_product', $attributeSet['attribute_set_name']);
        }
    }

    /**
     * Update attribute sets and groups
     *
     * @param array $data
     *
     * @return void
     */
    protected function _updateAttributeSets(array $data)
    {
        $entityTypeId = $this->_catalogProductEntityTypeId;
        foreach ($data as $attributeSet) {
            $attrSetName     = $attributeSet['attribute_set_name'];
            $sortOrder       = $attributeSet['sort_order'];
            $attributeGroups = $attributeSet;
            unset($attributeGroups['attribute_set_name']);
            unset($attributeGroups['sort_order']);

            $this->_setup->addAttributeSet($entityTypeId, $attrSetName, $sortOrder);

            $attrSetId = $this->_setup->getAttributeSet($entityTypeId, $attrSetName, 'attribute_set_id');

            $currentGroups = $this->_getAttributeGroups($attrSetId);

            $groupsToRemove = array_keys(array_diff_key($currentGroups, $attributeGroups));
            foreach ($groupsToRemove as $groupToRemoveName) {
                unset($currentGroups[$groupToRemoveName]);
            }

            foreach ($attributeGroups as $groupName => $groupSortOrder) {
                $this->_setup->addAttributeGroup($entityTypeId, $attrSetId, $groupName, $groupSortOrder);
            }
        }
    }

    /**
     * Remove attribute sets and attribute groups if not exist
     *
     * @param array $data
     *
     * @return void
     */
    protected function _pruneAttributeSets(array $data)
    {
        $entityTypeId         = $this->_catalogProductEntityTypeId;
        $magAttributeSetsName = $this->_getAttributeSetsNameAsArray();
        $attributeSetsName    = array();

        foreach ($data as $attributeSet) {
            $attributeSetsName[] = $attributeSet['attribute_set_name'];
        }

        $attributeSetsToRemove = array_diff($magAttributeSetsName, $attributeSetsName);
        foreach ($attributeSetsToRemove as $attributeSet) {
            $this->_setup->removeAttributeSet($entityTypeId, $attributeSet);
        }

        foreach ($data as $attributeSet) {
            $attrSetName     = $attributeSet['attribute_set_name'];
            $attributeGroups = $attributeSet;
            unset($attributeGroups['attribute_set_name']);
            unset($attributeGroups['sort_order']);

            $attrSetId = $this->_setup->getAttributeSet($entityTypeId, $attrSetName, 'attribute_set_id');

            $currentGroups = $this->_getAttributeGroups($attrSetId);

            $groupsToRemove = array_keys(array_diff_key($currentGroups, $attributeGroups));
            foreach ($groupsToRemove as $groupToRemoveName) {
                $this->_setup->removeAttributeGroup($entityTypeId, $attrSetId, $groupToRemoveName);
            }
        }
    }

    /**
     * Gives current attribute sets name as array
     * Returns ['name', ...]
     *
     * @return array
     */
    protected function _getAttributeSetsNameAsArray()
    {
        $attributeSetCollection = Mage::getModel('eav/entity_attribute_set')
            ->getCollection()
            ->setEntityTypeFilter($this->_catalogProductEntityTypeId);

        $attributeSetsName = array();
        foreach ($attributeSetCollection as $attrSet) {
            $attrSetAsArray      = $attrSet->getData();
            $attributeSetsName[] = $attrSetAsArray['attribute_set_name'];
        }

        return $attributeSetsName;
    }

    /**
     * Gives attribute groups which come from the given attribute set
     * Returns ['attribute group name' => 'sort order', ...]
     *
     * @param $attrSetId
     *
     * @return array
     */
    protected function _getAttributeGroups($attrSetId)
    {
        $connexion = $this->_setup->getConnection();
        $getOldGroupsQuery = $connexion
            ->select()
            ->from($this->_setup->getTable('eav/attribute_group'))
            ->where('attribute_set_id = :attribute_set_id');

        $bind = array('attribute_set_id' => $attrSetId);

        $currentGroups = array();
        foreach ($connexion->fetchAssoc($getOldGroupsQuery, $bind) as $attrGroup) {
            $currentGroups[$attrGroup['attribute_group_name']] = $attrGroup['sort_order'];
        }

        return $currentGroups;
    }

    /**
     * Sets entity type in the source model.
     *
     * @param string $entityType
     * @return void
     */
    protected function _setEntityTypeCode($entityType)
    {
        try {
            $this->_api->getDataSourceModel()->setEntityTypeCode($entityType);
        } catch(Mage_Core_Exception $e) {
            $this->_fault('invalid_entity_type', $e->getMessage());
        }
    }

    /**
     * Sets import behavior in the source model.
     *
     * @param string $behavior
     * @return void
     */
    protected function _setBehavior($behavior)
    {
        try {
            $this->_api->getDataSourceModel()->setBehavior($behavior);
        } catch(Mage_Core_Exception $e) {
            $this->_fault('invalid_behavior', $e->getMessage());
        }
    }
}
