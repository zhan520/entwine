/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#include <memory>
#include <string>
#include <vector>

#include <entwine/tree/config.hpp>
#include <entwine/types/outer-scope.hpp>

namespace Json { class Value; }

namespace entwine
{

namespace arbiter { class Arbiter; }

class Builder;

class Merger
{
public:
    Merger(const Config& config);
    ~Merger();

    void go();

private:
    Config m_config;
    OuterScope m_outerScope;

    std::unique_ptr<Builder> m_builder;
    std::vector<std::size_t> m_others;
};

} // namespace entwine

